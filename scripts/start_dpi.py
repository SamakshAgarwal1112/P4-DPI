#!/usr/bin/env python3
"""
Main startup script for P4 DPI Tool
Orchestrates the entire DPI system including Mininet, P4Runtime, and monitoring
"""

import os
import sys
import time
import signal
import logging
import threading
import subprocess
from datetime import datetime
from typing import Dict, List, Optional
import yaml
import json

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from p4_controller import DPIController
from packet_logger import PacketLogger
from traffic_generator import TrafficGenerator
from mininet_topology import DPITopology

class DPISystem:
    def __init__(self, config_file: str = "config/dpi_config.yaml"):
        """Initialize the DPI system"""
        self.config = self.load_config(config_file)
        self.logger = None
        self.setup_logging()
        self.running = False
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Initialize components
        self.controller = None
        self.traffic_generator = None
        self.topology = None
        
        # Threads
        self.threads = []
        
        # Process management
        self.processes = {}
        
    def load_config(self, config_file: str) -> dict:
        """Load system configuration"""
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_file}")
            return {}
    
    def setup_logging(self):
        """Setup system logging"""
        log_config = self.config.get('logging', {})
        
        # Create logs directory
        os.makedirs('logs', exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            handlers=[
                logging.FileHandler(log_config.get('file', 'logs/dpi_system.log')),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger('DPI_System')
        self.logger.info("DPI System initialized")
    
    def compile_p4_program(self):
        """Compile the P4 program"""
        self.logger.info("Compiling P4 program...")
        
        try:
            # Compile P4 program
            p4_file = 'p4_programs/dpi_l2_l4.p4'
            p4info_file = 'p4_programs/dpi_l2_l4.p4info.txt'
            json_file = 'p4_programs/dpi_l2_l4.json'
            
            # Check if P4 compiler is available
            result = subprocess.run(['p4c', '--version'], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                self.logger.error("P4 compiler not found. Please install P4 tools.")
                return False
            
            # Compile P4 program
            compile_cmd = [
                'p4c',
                '--target', 'bmv2',
                '--arch', 'v1model',
                '--p4runtime-files', p4info_file,
                '--std', 'p4_16',
                p4_file
            ]
            
            result = subprocess.run(compile_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                self.logger.error(f"P4 compilation failed: {result.stderr}")
                return False
            
            # Move JSON file to correct location
            if os.path.exists('dpi_l2_l4.json'):
                import shutil
                shutil.move('dpi_l2_l4.json', json_file)
            
            self.logger.info("P4 program compiled successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error compiling P4 program: {e}")
            return False
    
    def start_mininet_topology(self):
        """Start Mininet topology"""
        self.logger.info("Starting Mininet topology...")
        
        try:
            self.topology = DPITopology()
            
            # Start topology in a separate thread
            topology_thread = threading.Thread(
                target=self.topology.create_topology,
                daemon=True
            )
            topology_thread.start()
            self.threads.append(topology_thread)
            
            # Wait for topology to be ready
            time.sleep(10)
            
            # Start built-in Mininet-host traffic generation to exercise the pipeline
            try:
                self.topology.start_traffic_generation()
            except Exception as e:
                self.logger.warning(f"Could not start built-in Mininet traffic: {e}")

            self.logger.info("Mininet topology started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting Mininet topology: {e}")
            return False
    
    def start_p4_controller(self):
        """Start P4 controller"""
        self.logger.info("Starting P4 controllers (one per switch)...")
        try:
            switches = self.config.get('switches', [])
            if not switches:
                self.logger.warning("No switches defined in config")
                return True

            # Spawn a worker process per switch to avoid p4runtime_sh singleton limits
            for sw in switches:
                name = sw.get('name', 's1')
                device_id = str(sw.get('device_id', 1))
                grpc_port = str(sw.get('grpc_port', 50051))
                p4info = sw.get('p4info_file', 'p4_programs/dpi_l2_l4.p4info.txt')
                json_path = sw.get('runtime_json_file', 'p4_programs/dpi_l2_l4.json')

                cmd = [
                    'python3', 'scripts/p4_controller_worker.py',
                    '--name', name,
                    '--device-id', device_id,
                    '--grpc-port', grpc_port,
                    '--p4info', p4info,
                    '--json', json_path
                ]
                proc = subprocess.Popen(cmd, cwd=os.path.abspath('.'))
                self.processes[f'p4c_{name}'] = proc
                self.logger.info(f"Launched controller worker for {name} (PID {proc.pid})")

            time.sleep(3)
            self.logger.info("All P4 controller workers launched")
            return True
        except Exception as e:
            self.logger.error(f"Error starting P4 controller workers: {e}")
            return False
    
    def start_packet_logger(self):
        """Start packet logger"""
        self.logger.info("Starting packet logger...")
        
        try:
            self.logger_component = PacketLogger()
            self.logger.info("Packet logger started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting packet logger: {e}")
            return False
    
    def start_traffic_generator(self):
        """Start traffic generator"""
        self.logger.info("Starting traffic generator...")
        
        try:
            self.traffic_generator = TrafficGenerator()
            
            # Start traffic generation in a separate thread
            traffic_thread = threading.Thread(
                target=self.traffic_generator.start_traffic_generation,
                daemon=True
            )
            traffic_thread.start()
            self.threads.append(traffic_thread)
            
            self.logger.info("Traffic generator started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting traffic generator: {e}")
            return False
    
    def start_web_interface(self):
        """Start web interface"""
        self.logger.info("Starting Flask API server...")
        
        try:
            web_process = subprocess.Popen([
                'python3', 'scripts/flask_api.py',
                '--host', '0.0.0.0',
                '--port', '5000'
            ])
            self.processes['flask_api'] = web_process
            self.logger.info("Flask API server started")
            return True
        except Exception as e:
            self.logger.error(f"Error starting Flask API: {e}")
            return False
        # web_config = self.config.get('web_interface', {})
        # if not web_config.get('enabled', False):
        #     return True
        
        # self.logger.info("Starting web interface...")
        
        # try:
        #     # Start web interface
        #     web_script = 'scripts/web_interface.py'
        #     if os.path.exists(web_script):
        #         web_process = subprocess.Popen([
        #             'python3', web_script,
        #             '--host', web_config.get('host', '0.0.0.0'),
        #             '--port', str(web_config.get('port', 5000))
        #         ])
        #         self.processes['web_interface'] = web_process
        #         self.logger.info("Web interface started")
        #     else:
        #         self.logger.warning("Web interface script not found")
            
        #     return True
            
        # except Exception as e:
        #     self.logger.error(f"Error starting web interface: {e}")
        #     return False
    
    def start_monitoring(self):
        """Start monitoring and statistics collection"""
        self.logger.info("Starting monitoring...")
        
        try:
            # Start monitoring thread
            monitoring_thread = threading.Thread(
                target=self.monitor_system,
                daemon=True
            )
            monitoring_thread.start()
            self.threads.append(monitoring_thread)
            
            self.logger.info("Monitoring started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting monitoring: {e}")
            return False
    
    def monitor_system(self):
        """Monitor system health and performance"""
        while self.running:
            try:
                # Check system health
                self.check_system_health()
                
                # Log statistics
                self.log_statistics()
                
                # Sleep for monitoring interval
                time.sleep(self.config.get('monitoring', {}).get('stats_interval', 10))
                
            except Exception as e:
                self.logger.error(f"Error in monitoring: {e}")
    
    def check_system_health(self):
        """Check health of all system components"""
        health_status = {
            'timestamp': datetime.now().isoformat(),
            'components': {}
        }
        
        # Check controller
        if self.controller:
            health_status['components']['controller'] = 'running'
        else:
            health_status['components']['controller'] = 'stopped'
        
        # Check logger
        if hasattr(self, 'logger_component'):
            health_status['components']['logger'] = 'running'
        else:
            health_status['components']['logger'] = 'stopped'
        
        # Check traffic generator
        if self.traffic_generator:
            health_status['components']['traffic_generator'] = 'running'
        else:
            health_status['components']['traffic_generator'] = 'stopped'
        
        # Check topology
        if self.topology:
            health_status['components']['topology'] = 'running'
        else:
            health_status['components']['topology'] = 'stopped'
        
        # Save health status
        with open('logs/health_status.json', 'w') as f:
            json.dump(health_status, f, indent=2)
    
    def log_statistics(self):
        """Log system statistics"""
        if self.controller:
            stats = self.controller.get_real_time_stats()
            self.logger.info(f"System statistics: {stats}")
    
    def start_system(self):
        """Start the entire DPI system"""
        print("DEBUG: start_system called", flush=True)
        self.logger.info("Starting P4 DPI System...")
        self.running = True
        
        try:
            # Compile P4 program
            print("DEBUG: compiling P4...", flush=True)
            if not self.compile_p4_program():
                print("DEBUG: compile failed", flush=True)
                self.logger.error("Failed to compile P4 program")
                return False
            print("DEBUG: compile OK", flush=True)
            
            # Start components in order
            components = [
                ('Mininet Topology', self.start_mininet_topology),
                ('P4 Controller', self.start_p4_controller),
                ('Packet Logger', self.start_packet_logger),
                ('Traffic Generator', self.start_traffic_generator),
                ('Web Interface', self.start_web_interface),
                ('Monitoring', self.start_monitoring)
            ]
            
            for name, start_func in components:
                print(f"DEBUG: Starting {name}...", flush=True)
                self.logger.info(f"Starting {name}...")
                if not start_func():
                    print(f"DEBUG: {name} failed", flush=True)
                    self.logger.error(f"Failed to start {name}")
                    return False
                print(f"DEBUG: {name} OK", flush=True)
                time.sleep(2)  # Wait between components
            
            # Schedule a one-time export to produce a fresh JSON for this run
            try:
                self.schedule_initial_export()
            except Exception as e:
                self.logger.warning(f"Failed to schedule initial export: {e}")

            print("DEBUG: All components started", flush=True)
            self.logger.info("P4 DPI System started successfully")
            return True
            
        except Exception as e:
            print(f"DEBUG: Exception in start_system: {e}", flush=True)
            self.logger.error(f"Error starting system: {e}")
            return False
    
    def stop_system(self):
        """Stop the entire DPI system"""
        self.logger.info("Stopping P4 DPI System...")
        self.running = False
        
        try:
            # Stop traffic generator
            if self.traffic_generator:
                self.traffic_generator.stop_traffic_generation()
            
            # Stop controller
            if self.controller:
                self.controller.stop()
            
            # Stop packet logger
            if hasattr(self, 'logger_component'):
                self.logger_component.close()
            
            # Stop topology
            if self.topology:
                self.topology.stop()
            
            # Stop processes
            for name, process in self.processes.items():
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except:
                    process.kill()
            
            # Wait for threads to finish
            for thread in self.threads:
                thread.join(timeout=5)
            
            self.logger.info("P4 DPI System stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping system: {e}")
    
    def run(self):
        """Run the DPI system"""
        print("DEBUG: run() called", flush=True)
        try:
            print("DEBUG: calling start_system", flush=True)
            if self.start_system():
                print(f"DEBUG: start_system returned True, running={self.running}", flush=True)
                # Keep running until interrupted
                while self.running:
                    time.sleep(360)
                    break

                print(f"DEBUG: exited run loop, running={self.running}", flush=True)
            else:
                print("DEBUG: start_system returned False", flush=True)
                self.logger.error("Failed to start system")
                
        except KeyboardInterrupt:
            print("DEBUG: KeyboardInterrupt", flush=True)
            self.logger.info("Received interrupt signal")
        except Exception as e:
            print(f"DEBUG: Exception in run: {e}", flush=True)
            self.logger.error(f"Error in main loop: {e}")
        finally:
            print("DEBUG: run() finally block", flush=True)
            self.stop_system()

    def schedule_initial_export(self):
        """Export a fresh JSON/CSV snapshot for this run after a short delay."""
        delay = self.config.get('export', {}).get('initial_delay_seconds', 20)
        db_file = self.config.get('performance', {}).get('database', {}).get('file', 'logs/packets.db')

        def _export_after_delay():
            try:
                time.sleep(max(1, int(delay)))
                # Use the DB-based exporter to guarantee non-empty if packets exist in DB
                cmd = [
                    'python3', 'scripts/export_db.py',
                    '--db', db_file,
                    '--out', 'logs'
                ]
                subprocess.run(cmd, cwd=os.path.abspath('.'))
                self.logger.info("Initial run export completed")
            except Exception as e:
                self.logger.error(f"Initial export failed: {e}")

        t = threading.Thread(target=_export_after_delay, daemon=True)
        t.start()

def signal_handler(signum, frame):
    """Handle interrupt signals"""
    print("\nShutting down DPI System...")
    sys.exit(0)

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='P4 DPI System')
    parser.add_argument('--config', default='config/dpi_config.yaml',
                       help='Configuration file')
    parser.add_argument('--mode', choices=['start', 'stop', 'restart'], 
                       default='start', help='Operation mode')
    
    args = parser.parse_args()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run system
    dpi_system = DPISystem(args.config)
    
    if args.mode == 'start':
        dpi_system.run()
    elif args.mode == 'stop':
        dpi_system.stop_system()
    elif args.mode == 'restart':
        dpi_system.stop_system()
        time.sleep(2)
        dpi_system.run()

if __name__ == "__main__":
    main()
