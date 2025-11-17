#!/usr/bin/env python3
"""
Flask API Server for P4 DPI Dashboard
Provides REST API and WebSocket endpoints for real-time packet data streaming
File Location: Add this to your backend repo as scripts/flask_api.py
"""

import os
import sys
import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import threading
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DPI_API')

# Initialize Flask app
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Configuration
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'packets.db')
TIME_OFFSET_SECONDS = 360  # 6 minutes behind current time

class PacketDataProvider:
    """Handles database operations and data transformation"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        logger.info(f"Initialized PacketDataProvider with DB: {db_path}")
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
    
    def transform_packet(self, row: sqlite3.Row) -> Dict:
        """Transform database row to frontend format"""
        # Derive action from security flags
        action = 'dropped' if (row['is_suspicious'] or row['is_malformed']) else 'forwarded'
        
        return {
            'timestamp': row['timestamp'],
            'packet_id': row['id'],
            'source_ip': row['src_ip'] or 'N/A',
            'dest_ip': row['dst_ip'] or 'N/A',
            'source_port': row['src_port'] if row['src_port'] else 'N/A',
            'dest_port': row['dst_port'] if row['dst_port'] else 'N/A',
            'protocol': self._format_protocol(row),
            'packet_size': row['packet_size'] or 0,
            'action': action,
            # Additional fields for display
            'src_mac': row['src_mac'] or 'N/A',
            'dst_mac': row['dst_mac'] or 'N/A',
            'is_suspicious': bool(row['is_suspicious']),
            'is_malformed': bool(row['is_malformed']),
            'ttl': row['ttl'] or 0,
        }
    
    def _format_protocol(self, row: sqlite3.Row) -> str:
        """Format protocol information"""
        protocol_parts = []
        
        # Layer 4 protocol (TCP/UDP/ICMP)
        if row['layer4_protocol']:
            protocol_parts.append(row['layer4_protocol'].upper())
        elif row['protocol']:
            protocol_parts.append(row['protocol'].upper())
        
        # Add specific protocol info if available
        src_port = row['src_port']
        dst_port = row['dst_port']
        
        if dst_port == 80:
            protocol_parts.append('HTTP')
        elif dst_port == 443:
            protocol_parts.append('HTTPS')
        elif dst_port == 22:
            protocol_parts.append('SSH')
        elif dst_port == 53:
            protocol_parts.append('DNS')
        
        return '/'.join(protocol_parts) if protocol_parts else 'UNKNOWN'
    
    def get_packets_with_offset(self, offset_seconds: int = TIME_OFFSET_SECONDS, limit: int = 100) -> List[Dict]:
        """
        Fetch packets from database with time offset
        offset_seconds: How many seconds behind current time to fetch data
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Calculate target time (current time - offset)
            target_time = datetime.now() - timedelta(seconds=offset_seconds)
            target_time_str = target_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Query packets around the target time
            # Fetch packets within a 15-second window around target time
            start_time = (target_time - timedelta(seconds=7)).strftime('%Y-%m-%d %H:%M:%S')
            end_time = (target_time + timedelta(seconds=8)).strftime('%Y-%m-%d %H:%M:%S')
            
            query = """
                SELECT * FROM packets 
                WHERE timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            
            cursor.execute(query, (start_time, end_time, limit))
            rows = cursor.fetchall()
            
            packets = [self.transform_packet(row) for row in rows]
            
            conn.close()
            
            logger.info(f"Fetched {len(packets)} packets around {target_time_str}")
            return packets
            
        except Exception as e:
            logger.error(f"Error fetching packets: {e}")
            return []
    
    def get_latest_packets(self, limit: int = 50) -> List[Dict]:
        """Fetch latest packets from database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
                SELECT * FROM packets 
                ORDER BY timestamp DESC
                LIMIT ?
            """
            
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            
            packets = [self.transform_packet(row) for row in rows]
            
            conn.close()
            return packets
            
        except Exception as e:
            logger.error(f"Error fetching latest packets: {e}")
            return []
    
    def get_stats(self) -> Dict:
        """Get aggregated statistics"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get total packet count
            cursor.execute("SELECT COUNT(*) as total FROM packets")
            total = cursor.fetchone()['total']
            
            # Get suspicious/malformed counts
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN is_suspicious = 1 OR is_malformed = 1 THEN 1 ELSE 0 END) as dropped,
                    SUM(CASE WHEN is_suspicious = 0 AND is_malformed = 0 THEN 1 ELSE 0 END) as forwarded,
                    AVG(packet_size) as avg_size
                FROM packets
            """)
            stats_row = cursor.fetchone()
            
            conn.close()
            
            return {
                'total_packets': total,
                'forwarded': stats_row['forwarded'] or 0,
                'dropped': stats_row['dropped'] or 0,
                'avg_packet_size': round(stats_row['avg_size'] or 0, 2)
            }
            
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            return {
                'total_packets': 0,
                'forwarded': 0,
                'dropped': 0,
                'avg_packet_size': 0
            }

# Initialize data provider
data_provider = PacketDataProvider(DB_PATH)

# REST API Endpoints
@app.route('/api/packets', methods=['GET'])
def get_packets():
    """
    GET /api/packets
    Query params:
        - offset: Time offset in seconds (default: 360 = 6 minutes)
        - limit: Number of packets to return (default: 100)
    """
    try:
        offset = int(request.args.get('offset', TIME_OFFSET_SECONDS))
        limit = int(request.args.get('limit', 100))
        
        packets = data_provider.get_packets_with_offset(offset, limit)
        
        return jsonify({
            'success': True,
            'count': len(packets),
            'offset_seconds': offset,
            'data': packets
        })
    
    except Exception as e:
        logger.error(f"Error in /api/packets: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': []
        }), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """GET /api/stats - Get aggregated statistics"""
    try:
        stats = data_provider.get_stats()
        return jsonify({
            'success': True,
            'data': stats
        })
    except Exception as e:
        logger.error(f"Error in /api/stats: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check database connectivity
        conn = data_provider.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM packets")
        count = cursor.fetchone()[0]
        conn.close()
        
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'packet_count': count
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

# WebSocket Events
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info(f"Client connected: {request.sid}")
    emit('connection_response', {'status': 'connected', 'message': 'Connected to DPI API'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('request_packets')
def handle_packet_request(data):
    """
    Handle packet data request from client
    Client sends: { 'offset': 360 } (6 minutes)
    """
    try:
        offset = data.get('offset', TIME_OFFSET_SECONDS)
        packets = data_provider.get_packets_with_offset(offset, limit=100)
        
        emit('packets_update', {
            'timestamp': datetime.now().isoformat(),
            'offset_seconds': offset,
            'count': len(packets),
            'packets': packets
        })
    except Exception as e:
        logger.error(f"Error handling packet request: {e}")
        emit('error', {'message': str(e)})

# Background thread for continuous data streaming
streaming_active = False
streaming_thread = None

def background_stream():
    """Background thread to stream data continuously"""
    global streaming_active
    logger.info("Background streaming thread started")
    
    while streaming_active:
        try:
            # Calculate current offset (6 minutes behind)
            current_time = datetime.now()
            offset_seconds = TIME_OFFSET_SECONDS
            
            # Fetch packets with offset
            packets = data_provider.get_packets_with_offset(offset_seconds, limit=50)
            
            if packets:
                # Emit to all connected clients
                socketio.emit('packets_stream', {
                    'timestamp': current_time.isoformat(),
                    'offset_seconds': offset_seconds,
                    'target_time': (current_time - timedelta(seconds=offset_seconds)).isoformat(),
                    'count': len(packets),
                    'packets': packets
                }, broadcast=True)
                
                logger.info(f"Streamed {len(packets)} packets to clients")
            else:
                logger.info("No packets found for current time window")
            
            # Sleep for 5 seconds before next update
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error in background stream: {e}")
            time.sleep(5)
    
    logger.info("Background streaming thread stopped")

@socketio.on('start_stream')
def handle_start_stream():
    """Start continuous data streaming"""
    global streaming_active, streaming_thread
    
    if not streaming_active:
        streaming_active = True
        streaming_thread = threading.Thread(target=background_stream, daemon=True)
        streaming_thread.start()
        logger.info("Started continuous streaming")
        emit('stream_status', {'status': 'streaming', 'message': 'Continuous streaming started'})
    else:
        emit('stream_status', {'status': 'already_streaming', 'message': 'Streaming already active'})

@socketio.on('stop_stream')
def handle_stop_stream():
    """Stop continuous data streaming"""
    global streaming_active
    
    if streaming_active:
        streaming_active = False
        logger.info("Stopped continuous streaming")
        emit('stream_status', {'status': 'stopped', 'message': 'Continuous streaming stopped'})
    else:
        emit('stream_status', {'status': 'not_streaming', 'message': 'Streaming not active'})

def main():
    """Main function to run the Flask API server"""
    import argparse
    
    parser = argparse.ArgumentParser(description='P4 DPI Flask API Server')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    args = parser.parse_args()
    
    # Check if database exists
    if not os.path.exists(DB_PATH):
        logger.warning(f"Database not found at {DB_PATH}")
        logger.warning("API will start but may return empty data until database is available")
    else:
        logger.info(f"Database found at {DB_PATH}")
    
    logger.info(f"Starting Flask API server on {args.host}:{args.port}")
    logger.info(f"Time offset: {TIME_OFFSET_SECONDS} seconds (6 minutes)")
    
    # Run the server
    socketio.run(app, host=args.host, port=args.port, debug=args.debug, allow_unsafe_werkzeug=True)

if __name__ == '__main__':
    main()
