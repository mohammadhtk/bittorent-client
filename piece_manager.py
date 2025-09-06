import threading
from typing import Dict, List, Set, Optional, Callable
from utils import sha1_hash

# Standard block size for BitTorrent (16KB)
BLOCK_SIZE = 16384

# Represents a block within a piece
class Block:
    def __init__(self, piece_index: int, offset: int, length: int):
        self.piece_index = piece_index
        self.offset = offset
        self.length = length
        self.data = None
        self.requested = False
        self.received = False

# Represents a piece with its blocks
class Piece:
    def __init__(self, index: int, length: int, hash_value: bytes):
        self.index = index
        self.length = length
        self.hash_value = hash_value
        self.blocks = []
        self.completed = False
        self.verified = False
        self.data = bytearray(length)
        
        # Create blocks for this piece
        self._create_blocks()

    # Create blocks for this piece
    def _create_blocks(self):
        offset = 0
        while offset < self.length:
            block_length = min(BLOCK_SIZE, self.length - offset)
            block = Block(self.index, offset, block_length)
            self.blocks.append(block)
            offset += block_length

    # Add block data to the piece
    def add_block_data(self, offset: int, data: bytes) -> bool:
        if offset + len(data) > self.length:
            return False
        
        # Find the corresponding block
        for block in self.blocks:
            if block.offset == offset and block.length == len(data):
                if not block.received:
                    block.data = data
                    block.received = True
                    
                    # Copy data to piece buffer
                    self.data[offset:offset + len(data)] = data
                    
                    # Check if piece is complete
                    if self.is_complete():
                        self.completed = True
                        self.verified = self.verify()
                    
                    return True
        
        return False

    # Check if all blocks have been received
    def is_complete(self) -> bool:
        return all(block.received for block in self.blocks)

    # Verify piece integrity using SHA1 hash
    def verify(self) -> bool:
        if not self.completed:
            return False
        
        calculated_hash = sha1_hash(bytes(self.data))
        return calculated_hash == self.hash_value

    # Get list of blocks that haven't been received yet
    def get_missing_blocks(self) -> List[Block]:
        return [block for block in self.blocks if not block.received and not block.requested]

    # Get list of blocks that have been requested but not received
    def get_requested_blocks(self) -> List[Block]:
        return [block for block in self.blocks if block.requested and not block.received]

    # Reset all block request flags (for timeout handling)
    def reset_block_requests(self):
        for block in self.blocks:
            if not block.received:
                block.requested = False

# Manages piece downloading and verification
class PieceManager:
    def __init__(self, torrent_file):
        self.torrent = torrent_file
        self.pieces = {}
        self.completed_pieces = set()
        self.lock = threading.Lock()
        
        # Callbacks
        self.on_piece_completed = None  # Callback when piece is completed and verified
        
        # Initialize pieces
        self._initialize_pieces()

    # Initialize all pieces
    def _initialize_pieces(self):
        print(f"Initializing {self.torrent.get_total_pieces()} pieces")
        
        for i in range(self.torrent.get_total_pieces()):
            piece_length = self.torrent.get_piece_length(i)
            piece_hash = self.torrent.get_piece_hash(i)
            
            piece = Piece(i, piece_length, piece_hash)
            self.pieces[i] = piece

    # Add piece data and check for completion
    def add_piece_data(self, piece_index: int, offset: int, data: bytes) -> bool:
        with self.lock:
            if piece_index not in self.pieces:
                return False
            
            piece = self.pieces[piece_index]
            if piece.completed:
                return True  # Already completed
            
            # Add block data to piece
            success = piece.add_block_data(offset, data)
            
            if success and piece.completed:
                if piece.verified:
                    print(f"Piece {piece_index} completed and verified!")
                    self.completed_pieces.add(piece_index)
                    
                    # Call completion callback
                    if self.on_piece_completed:
                        self.on_piece_completed(piece_index, bytes(piece.data))
                else:
                    print(f"Piece {piece_index} completed but failed verification!")
                    # Reset piece for re-download
                    piece.completed = False
                    piece.verified = False
                    piece.data = bytearray(piece.length)
                    for block in piece.blocks:
                        block.data = None
                        block.received = False
                        block.requested = False
            
            return success

    # Get next block request (piece_index, offset, length)
    def get_next_request(self, available_pieces: Set[int]) -> Optional[tuple]:
        with self.lock:
            # Find pieces we need that are available from peers
            needed_pieces = []
            for piece_index in available_pieces:
                if (piece_index in self.pieces and 
                    piece_index not in self.completed_pieces):
                    needed_pieces.append(piece_index)
            
            if not needed_pieces:
                return None
            
            # Prioritize pieces with fewer missing blocks (rarest first approximation)
            needed_pieces.sort(key=lambda p: len(self.pieces[p].get_missing_blocks()))
            
            # Find next block to request
            for piece_index in needed_pieces:
                piece = self.pieces[piece_index]
                missing_blocks = piece.get_missing_blocks()
                
                if missing_blocks:
                    block = missing_blocks[0]
                    block.requested = True
                    return (piece_index, block.offset, block.length)
            
            return None

    # Mark a block as requested
    def mark_block_requested(self, piece_index: int, offset: int):
        with self.lock:
            if piece_index in self.pieces:
                piece = self.pieces[piece_index]
                for block in piece.blocks:
                    if block.offset == offset:
                        block.requested = True
                        break

    # Reset all requests for a piece (for timeout handling)
    def reset_piece_requests(self, piece_index: int):
        with self.lock:
            if piece_index in self.pieces:
                self.pieces[piece_index].reset_block_requests()

    # Check if all pieces are completed
    def is_complete(self) -> bool:
        return len(self.completed_pieces) == len(self.pieces)

    # Get download completion percentage
    def get_completion_percentage(self) -> float:
        if not self.pieces:
            return 0.0
        return (len(self.completed_pieces) / len(self.pieces)) * 100.0

    # Get set of completed piece indices
    def get_completed_pieces(self) -> Set[int]:
        return self.completed_pieces.copy()

    # Get data for a completed piece
    def get_piece_data(self, piece_index: int) -> Optional[bytes]:
        with self.lock:
            if (piece_index in self.completed_pieces and 
                piece_index in self.pieces):
                return bytes(self.pieces[piece_index].data)
            return None

    # Get download statistics
    def get_download_stats(self) -> Dict:
        with self.lock:
            total_pieces = len(self.pieces)
            completed_pieces = len(self.completed_pieces)
            
            # Calculate bytes downloaded
            bytes_downloaded = 0
            for piece_index in self.completed_pieces:
                bytes_downloaded += self.pieces[piece_index].length
            
            return {
                'total_pieces': total_pieces,
                'completed_pieces': completed_pieces,
                'completion_percentage': self.get_completion_percentage(),
                'bytes_downloaded': bytes_downloaded,
                'total_bytes': self.torrent.total_length
            }

