# =========================================================
# data_scheduler.py - AUTOMATED DATA COLLECTION SCHEDULER
# Fixed version with proper database schema handling
# =========================================================

import schedule
import time
import threading
import json
import os
import sqlite3
import zipfile
import shutil
import hashlib
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Import from your existing modules
# FIRST: Import ZambianMarketData directly BEFORE importing from app
try:
    from zambian_data import ZambianMarketData
    ZAMBIAN_MODULE_AVAILABLE = True
    print("✅ ZambianMarketData imported successfully for scheduler")
except ImportError as e:
    print(f"⚠️  Could not import ZambianMarketData from zambian_data: {e}")
    ZAMBIAN_MODULE_AVAILABLE = False
    # Define minimal fallback class
    class ZambianMarketData:
        ZAMBIAN_MARKETS = {}
        COMMODITY_PRICE_RANGES = {}
        SEASONAL_CALENDAR = {}
        
        @staticmethod
        def fetch_all_sources():
            return []
# =========================================================
# DATA SCHEDULER CLASS
# =========================================================

class DataScheduler:
    """Automated data collection and management scheduler"""
    
    def __init__(self):
        self.running = False
        self.schedule_thread = None
        self.backup_dir = "backups"
        self.log_dir = "logs"
        
        # Create necessary directories
        self._create_directories()
        
        # Initialize data collector - use directly imported ZambianMarketData
        if ZAMBIAN_MODULE_AVAILABLE:
            self.market_data = ZambianMarketData()
        else:
            print("⚠️  Running in simulation mode - using mock data")
            self.market_data = MockZambianData()
    
    # Other methods remain the same...

# =========================================================
# DATA SCHEDULER CLASS
# =========================================================

class DataScheduler:
    """Automated data collection and management scheduler"""
    
    def __init__(self):
        self.running = False
        self.schedule_thread = None
        self.backup_dir = "backups"
        self.log_dir = "logs"
        
        # Create necessary directories
        self._create_directories()
        
        # Initialize data collector
        if ZAMBIAN_MODULE_AVAILABLE:
            self.market_data = ZambianMarketData()
        else:
            print("⚠️  Running in simulation mode - using mock data")
            self.market_data = MockZambianData()
    
    def _create_directories(self):
        """Create necessary directories for logs and backups"""
        for directory in [self.backup_dir, self.log_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                print(f"📁 Created directory: {directory}")
    
    # =====================================================
    # DATA COLLECTION JOBS
    # =====================================================
    
    def collect_daily_data(self):
        """Daily data collection job - runs at 8:00 AM"""
        print(f"\n{'='*60}")
        print(f"🌍 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DAILY ZAMBIAN MARKET DATA COLLECTION")
        print(f"{'='*60}")
        
        try:
            start_time = time.time()
            
            # Collect from all Zambian sources
            print("📊 Collecting data from Zambian sources...")
            prices = self.market_data.fetch_all_sources()
            
            # Save to database with schema-safe method
            saved_count = self._save_prices_to_database_safe(prices)
            
            duration = time.time() - start_time
            
            # Log collection
            self._log_collection_activity(
                operation="daily_collection",
                records_collected=saved_count,
                duration=duration,
                status="success" if saved_count > 0 else "partial"
            )
            
            # Send notification
            self._notify_admin(
                f"Daily collection completed: {saved_count} records ({duration:.1f}s)"
            )
            
            print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Daily collection: {saved_count} records in {duration:.1f}s")
            print(f"{'='*60}")
            
        except Exception as e:
            error_msg = f"Daily collection failed: {str(e)}"
            print(f"❌ {error_msg}")
            
            self._log_collection_activity(
                operation="daily_collection",
                records_collected=0,
                duration=0,
                status="failed",
                error_message=error_msg
            )
            
            self._notify_admin(error_msg)
    
    def collect_hourly_updates(self):
        """Hourly price updates - runs at :30 past each hour (8:30-17:30)"""
        current_hour = datetime.now().hour
        
        # Only run during market hours (8 AM to 6 PM)
        if 8 <= current_hour < 18:
            print(f"⏰ [{datetime.now().strftime('%H:%M:%S')}] Hourly market update...")
            
            try:
                # Collect from primary sources only
                prices = []
                
                if ZAMBIAN_MODULE_AVAILABLE:
                    # Get ZNFU prices (most reliable)
                    znfu_prices = self.market_data.fetch_znfu_prices()
                    if znfu_prices:
                        prices.extend(znfu_prices[:15])  # Limit to 15 records
                    
                    # Get Ministry of Agriculture prices
                    maco_prices = self.market_data.fetch_maco_prices()
                    if maco_prices:
                        prices.extend(maco_prices[:10])  # Limit to 10 records
                else:
                    # Generate mock data for simulation
                    prices = self.market_data.generate_hourly_update()
                
                # Save to database with schema-safe method
                saved_count = self._save_prices_to_database_safe(prices)
                
                # Log the update
                self._log_collection_activity(
                    operation="hourly_update",
                    records_collected=saved_count,
                    duration=0,
                    status="success"
                )
                
                print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Hourly update: {saved_count} records")
                
            except Exception as e:
                print(f"⚠️  Hourly update failed: {e}")
    
    def update_market_status(self):
        """Update market status and metadata - runs at 7:00 AM"""
        print(f"🏪 [{datetime.now().strftime('%H:%M:%S')}] Updating market status...")
        
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Get current day
            today = datetime.now().strftime("%A")
            
            # Update market status based on market days
            cur.execute("""
                SELECT name, region, market_days FROM markets WHERE active = 1
            """)
            markets = cur.fetchall()
            
            # Check if is_open_today column exists
            cur.execute("PRAGMA table_info(markets)")
            columns = [col[1] for col in cur.fetchall()]
            
            for market in markets:
                name, region, market_days = market
                market_days_list = market_days.split(',') if market_days else []
                
                # Check if today is a market day
                is_open = today in market_days_list
                
                # Update last updated timestamp
                if 'is_open_today' in columns:
                    cur.execute("""
                        UPDATE markets 
                        SET last_updated = ?, is_open_today = ?
                        WHERE name = ?
                    """, (datetime.now().isoformat(), int(is_open), name))
                else:
                    cur.execute("""
                        UPDATE markets 
                        SET last_updated = ?
                        WHERE name = ?
                    """, (datetime.now().isoformat(), name))
            
            conn.commit()
            conn.close()
            
            print(f"✅ Market status updated for {len(markets)} markets")
            
        except Exception as e:
            print(f"⚠️  Market status update failed: {e}")
    
    def send_daily_summaries(self):
        """Send daily SMS summaries to users - runs at 7:00 AM"""
        print(f"📱 [{datetime.now().strftime('%H:%M:%S')}] Sending daily SMS summaries...")
        
        try:
            # Import SMS service
            from app import sms_service
            
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Get users with SMS alerts enabled
            cur.execute("""
                SELECT user_id, phone, name, location, main_crops 
                FROM users 
                WHERE sms_alerts = 1 AND status = 'active'
            """)
            users = cur.fetchall()
            
            sent_count = 0
            for user in users:
                user_id, phone, name, location, main_crops = user
                
                try:
                    # Get user's main crops
                    crops = main_crops.split(',')[:3] if main_crops else ["Maize", "Tomatoes"]
                    
                    # Get latest prices for user's crops
                    message = f"FarmConnect Daily for {name}:\n"
                    
                    for crop in crops:
                        crop = crop.strip()
                        cur.execute("""
                            SELECT price, market FROM market_prices 
                            WHERE commodity = ? AND verified = 1 
                            ORDER BY recorded_at DESC LIMIT 1
                        """, (crop,))
                        price_data = cur.fetchone()
                        
                        if price_data:
                            message += f"{crop}: ZMW {price_data[0]} at {price_data[1]}\n"
                        else:
                            message += f"{crop}: No data\n"
                    
                    # Add footer
                    message += f"\nMarket in {location} active today. Dial *123# for live prices."
                    
                    # Send SMS (demo or real)
                    result = sms_service.send_sms(phone, message)
                    if result.get("success"):
                        sent_count += 1
                        
                except Exception as e:
                    print(f"⚠️  Failed to send to {phone}: {e}")
            
            conn.close()
            
            print(f"✅ Sent {sent_count} daily summaries")
            
        except Exception as e:
            print(f"⚠️  Daily summaries failed: {e}")
    
    # =====================================================
    # BACKUP AND MAINTENANCE JOBS
    # =====================================================
    
    def create_daily_backup(self):
        """Create daily database backup - runs at 2:00 AM"""
        print(f"💾 [{datetime.now().strftime('%H:%M:%S')}] Creating daily backup...")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"farmconnect_backup_{timestamp}"
            zip_path = os.path.join(self.backup_dir, f"{backup_name}.zip")
            
            # Create ZIP archive
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Backup database
                db_path = "farm_market.db"
                if os.path.exists(db_path):
                    zipf.write(db_path, "farm_market.db")
                    print(f"✅ Database backed up: {os.path.getsize(db_path)} bytes")
                
                # Backup configuration
                config_files = ['config.json', '.env', 'requirements.txt']
                for config_file in config_files:
                    if os.path.exists(config_file):
                        zipf.write(config_file, config_file)
                
                # Backup logs
                log_files = [f for f in os.listdir(self.log_dir) if f.endswith('.log')]
                for log_file in log_files[:5]:  # Limit to 5 most recent logs
                    log_path = os.path.join(self.log_dir, log_file)
                    zipf.write(log_path, f"logs/{log_file}")
            
            # Clean up old backups (keep last 30 days)
            self._cleanup_old_backups()
            
            print(f"✅ Backup created: {backup_name}.zip ({os.path.getsize(zip_path)} bytes)")
            
            # Log backup activity
            self._log_system_activity(
                action="daily_backup",
                details=f"Backup created: {backup_name}.zip"
            )
            
        except Exception as e:
            print(f"❌ Backup failed: {e}")
    
    def cleanup_old_data(self):
        """Clean up old data - runs weekly on Sunday at 1:00 AM"""
        print(f"🧹 [{datetime.now().strftime('%H:%M:%S')}] Cleaning up old data...")
        
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Delete prices older than 180 days
            cutoff_date = (datetime.now() - timedelta(days=180)).isoformat()
            cur.execute("""
                DELETE FROM market_prices 
                WHERE recorded_at < ?
            """, (cutoff_date,))
            
            deleted_prices = cur.rowcount
            
            # Delete old logs if table exists
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='collection_logs'")
            if cur.fetchone():
                cutoff_date = (datetime.now() - timedelta(days=90)).isoformat()
                cur.execute("""
                    DELETE FROM collection_logs 
                    WHERE collected_at < ?
                """, (cutoff_date,))
                deleted_logs = cur.rowcount
            else:
                deleted_logs = 0
            
            # Delete old SMS history if table exists
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sms_history'")
            if cur.fetchone():
                cutoff_date = (datetime.now() - timedelta(days=60)).isoformat()
                cur.execute("""
                    DELETE FROM sms_history 
                    WHERE sent_at < ?
                """, (cutoff_date,))
                deleted_sms = cur.rowcount
            else:
                deleted_sms = 0
            
            conn.commit()
            conn.close()
            
            print(f"✅ Cleanup complete:")
            print(f"   - Deleted {deleted_prices} old prices")
            print(f"   - Deleted {deleted_logs} old logs")
            print(f"   - Deleted {deleted_sms} old SMS records")
            
            self._log_system_activity(
                action="data_cleanup",
                details=f"Deleted {deleted_prices} prices, {deleted_logs} logs, {deleted_sms} SMS"
            )
            
        except Exception as e:
            print(f"⚠️  Data cleanup failed: {e}")
    
    def generate_daily_report(self):
        """Generate daily system report - runs at 11:00 PM"""
        print(f"📈 [{datetime.now().strftime('%H:%M:%S')}] Generating daily report...")
        
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Get statistics for the day
            today = datetime.now().strftime("%Y-%m-%d")
            
            # User statistics
            cur.execute("SELECT COUNT(*) FROM users WHERE status='active'")
            total_users = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM users WHERE created_at LIKE ?", (f"{today}%",))
            new_users_today = cur.fetchone()[0]
            
            # Price statistics
            cur.execute("SELECT COUNT(*) FROM market_prices WHERE recorded_at LIKE ?", (f"{today}%",))
            prices_today = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM market_prices WHERE verified=1")
            total_verified = cur.fetchone()[0]
            
            # SMS statistics if table exists
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sms_history'")
            if cur.fetchone():
                cur.execute("SELECT COUNT(*) FROM sms_history WHERE sent_at LIKE ?", (f"{today}%",))
                sms_today = cur.fetchone()[0]
            else:
                sms_today = 0
            
            # Generate report
            report = {
                "date": today,
                "users": {
                    "total": total_users,
                    "new_today": new_users_today
                },
                "prices": {
                    "collected_today": prices_today,
                    "total_verified": total_verified
                },
                "sms": {
                    "sent_today": sms_today
                },
                "generated_at": datetime.now().isoformat()
            }
            
            # Save report to file
            report_path = os.path.join(self.log_dir, f"daily_report_{today}.json")
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            conn.close()
            
            print(f"✅ Daily report saved: {report_path}")
            
            self._log_system_activity(
                action="daily_report",
                details=f"Report generated for {today}"
            )
            
        except Exception as e:
            print(f"⚠️  Daily report failed: {e}")
    
    # =====================================================
    # SCHEDULER MANAGEMENT
    # =====================================================
    
    def start_scheduler(self):
        """Start all scheduled jobs"""
        if self.running:
            print("⚠️  Scheduler already running")
            return
        
        print("🚀 Starting data scheduler...")
        
        # Clear existing schedules
        schedule.clear()
        
        # ============ DAILY JOBS ============
        
        # 7:00 AM - Update market status and send summaries
        schedule.every().day.at("07:00").do(self.update_market_status)
        schedule.every().day.at("07:05").do(self.send_daily_summaries)
        
        # 8:00 AM - Daily data collection (main job)
        schedule.every().day.at("08:00").do(self.collect_daily_data)
        
        # 2:00 AM - Daily backup
        schedule.every().day.at("02:00").do(self.create_daily_backup)
        
        # 11:00 PM - Daily report
        schedule.every().day.at("23:00").do(self.generate_daily_report)
        
        # ============ HOURLY JOBS ============
        
        # Hourly market updates (8:30 AM to 5:30 PM, market hours)
        for hour in range(8, 18):
            schedule.every().day.at(f"{hour:02d}:30").do(self.collect_hourly_updates)
        
        # ============ WEEKLY JOBS ============
        
        # Sunday 1:00 AM - Data cleanup
        schedule.every().sunday.at("01:00").do(self.cleanup_old_data)
        
        # ============ MONTHLY JOBS ============
        
        # First day of month - Generate monthly report
        schedule.every().monday.at("03:00").do(self.generate_monthly_report)
        
        # Start the scheduler thread
        self.running = True
        self.schedule_thread = threading.Thread(target=self._run_scheduler_loop, daemon=True)
        self.schedule_thread.start()
        
        print("✅ Data scheduler started successfully!")
        print("\n📅 Scheduled Jobs:")
        for job in schedule.get_jobs():
            print(f"   • {job}")
        print()
        
        # Log scheduler start
        self._log_system_activity("scheduler_start", "Data scheduler started")
    
    def _run_scheduler_loop(self):
        """Run the scheduler loop in background thread"""
        print("⏰ Scheduler loop running...")
        
        while self.running:
            try:
                schedule.run_pending()
            except Exception as e:
                print(f"⚠️  Scheduler error: {e}")
            
            time.sleep(60)  # Check every minute
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        print("🛑 Stopping data scheduler...")
        
        self.running = False
        schedule.clear()
        
        if self.schedule_thread:
            self.schedule_thread.join(timeout=5)
        
        print("✅ Data scheduler stopped")
        self._log_system_activity("scheduler_stop", "Data scheduler stopped")
    
    def run_once_now(self, job_name):
        """Run a specific job immediately"""
        print(f"▶️  Running {job_name} now...")
        
        if job_name == "daily":
            self.collect_daily_data()
        elif job_name == "hourly":
            self.collect_hourly_updates()
        elif job_name == "backup":
            self.create_daily_backup()
        elif job_name == "market_status":
            self.update_market_status()
        elif job_name == "cleanup":
            self.cleanup_old_data()
        elif job_name == "report":
            self.generate_daily_report()
        elif job_name == "summaries":
            self.send_daily_summaries()
        else:
            print(f"⚠️  Unknown job: {job_name}")
    
    # =====================================================
    # DATABASE HELPER METHODS (Schema-safe)
    # =====================================================
    
    def _get_db_connection(self):
        """Get database connection"""
        conn = sqlite3.connect('farm_market.db')
        conn.row_factory = sqlite3.Row
        return conn
    
    def _save_prices_to_database_safe(self, prices: List[Dict]) -> int:
        """
        Save prices to database with schema-safe approach
        Handles missing columns gracefully
        """
        if not prices:
            return 0
        
        saved_count = 0
        conn = self._get_db_connection()
        cur = conn.cursor()
        
        # Get table columns
        cur.execute("PRAGMA table_info(market_prices)")
        columns = [col[1] for col in cur.fetchall()]
        
        for price in prices:
            try:
                # Build safe insert based on available columns
                # Always try basic columns first
                market = price.get('market', 'Unknown Market')
                commodity = price.get('commodity', 'Unknown')
                price_value = price.get('price', 0.0)
                unit = price.get('unit', 'ZMW/kg')
                source = price.get('source', 'Zambian_Source')
                verified = price.get('verified', 1)
                recorded_at = price.get('recorded_at', datetime.now().isoformat())
                
                # Check if record exists for today
                today = datetime.now().strftime("%Y-%m-%d")
                cur.execute("""
                    SELECT id FROM market_prices 
                    WHERE market = ? AND commodity = ? AND date(recorded_at) = ?
                """, (market, commodity, today))
                
                existing_record = cur.fetchone()
                
                if existing_record:
                    # Update existing record
                    update_query = """
                        UPDATE market_prices 
                        SET price = ?, unit = ?, source = ?, verified = ?, recorded_at = ?
                    """
                    update_params = [price_value, unit, source, verified, recorded_at]
                    
                    # Add optional columns if they exist
                    if 'volume' in columns and 'volume' in price:
                        update_query += ", volume = ?"
                        update_params.append(price.get('volume'))
                    
                    if 'quality' in columns and 'quality' in price:
                        update_query += ", quality = ?"
                        update_params.append(price.get('quality'))
                    
                    if 'region' in columns and 'region' in price:
                        update_query += ", region = ?"
                        update_params.append(price.get('region'))
                    
                    if 'price_trend' in columns and 'price_trend' in price:
                        update_query += ", price_trend = ?"
                        update_params.append(price.get('price_trend'))
                    
                    # Add WHERE clause
                    update_query += " WHERE id = ?"
                    update_params.append(existing_record['id'])
                    
                    cur.execute(update_query, update_params)
                else:
                    # Insert new record
                    insert_cols = ["market", "commodity", "price", "unit", "source", "verified", "recorded_at"]
                    insert_vals = [market, commodity, price_value, unit, source, verified, recorded_at]
                    placeholders = ["?"] * len(insert_vals)
                    
                    # Add optional columns if they exist in table
                    optional_cols = [
                        ('volume', 'volume'),
                        ('quality', 'quality'),
                        ('region', 'region'),
                        ('market_lat', 'market_lat'),
                        ('market_lon', 'market_lon'),
                        ('price_trend', 'price_trend'),
                        ('collected_at', 'collected_at')
                    ]
                    
                    for price_key, col_name in optional_cols:
                        if col_name in columns and price_key in price:
                            insert_cols.append(col_name)
                            insert_vals.append(price[price_key])
                            placeholders.append("?")
                    
                    # Build and execute insert
                    insert_query = f"""
                        INSERT INTO market_prices ({', '.join(insert_cols)})
                        VALUES ({', '.join(placeholders)})
                    """
                    
                    cur.execute(insert_query, insert_vals)
                
                saved_count += 1
                
            except Exception as e:
                print(f"⚠️  Error saving price record: {e}")
                # Try simpler insert as fallback
                try:
                    cur.execute("""
                        INSERT INTO market_prices (market, commodity, price, unit, source, verified, recorded_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (market, commodity, price_value, unit, source, verified, recorded_at))
                    saved_count += 1
                except Exception as e2:
                    print(f"⚠️  Even simple insert failed: {e2}")
                    continue
        
        conn.commit()
        conn.close()
        
        return saved_count
    
    def _log_collection_activity(self, operation: str, records_collected: int, 
                                duration: float, status: str, error_message: str = None):
        """Log data collection activity - creates table if needed"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS collection_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT,
                    operation TEXT,
                    records_collected INTEGER,
                    status TEXT,
                    error_message TEXT,
                    duration_seconds REAL,
                    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                INSERT INTO collection_logs 
                (source_name, operation, records_collected, status, error_message, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                "Zambian_Market_Data",
                operation,
                records_collected,
                status,
                error_message,
                duration
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error logging collection: {e}")
    
    def _log_system_activity(self, action: str, details: str = None):
        """Log system activity - creates table if needed"""
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user TEXT,
                    action TEXT,
                    details TEXT,
                    ip_address TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                INSERT INTO activity_logs (user, action, details, ip_address, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                "system",
                action,
                details,
                "127.0.0.1",
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error logging system activity: {e}")
    
    def _notify_admin(self, message: str):
        """Send notification to admin - creates table if needed"""
        print(f"🔔 Admin Notification: {message}")
        
        try:
            conn = self._get_db_connection()
            cur = conn.cursor()
            
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT NOT NULL,
                    type TEXT DEFAULT 'info',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    read_status INTEGER DEFAULT 0
                )
            """)
            
            cur.execute("""
                INSERT INTO admin_notifications (message, type, created_at)
                VALUES (?, ?, ?)
            """, (
                message,
                'data_collection',
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error logging admin notification: {e}")
    
    def _cleanup_old_backups(self):
        """Clean up old backup files"""
        try:
            backup_files = []
            
            for filename in os.listdir(self.backup_dir):
                if filename.endswith('.zip'):
                    filepath = os.path.join(self.backup_dir, filename)
                    backup_files.append({
                        "path": filepath,
                        "mtime": os.path.getmtime(filepath)
                    })
            
            # Sort by modification time (oldest first)
            backup_files.sort(key=lambda x: x["mtime"])
            
            # Keep only last 30 backups
            if len(backup_files) > 30:
                files_to_remove = backup_files[:len(backup_files) - 30]
                
                for file_info in files_to_remove:
                    try:
                        os.remove(file_info["path"])
                        print(f"🧹 Removed old backup: {os.path.basename(file_info['path'])}")
                    except Exception as e:
                        print(f"⚠️  Error removing backup: {e}")
                        
        except Exception as e:
            print(f"Backup cleanup failed: {e}")
    
    def generate_monthly_report(self):
        """Generate monthly system report"""
        print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] Generating monthly report...")
        
        try:
            # Get month and year
            month_year = datetime.now().strftime("%Y-%m")
            
            # Generate report (simplified for now)
            report = {
                "month": month_year,
                "generated_at": datetime.now().isoformat(),
                "note": "Monthly report functionality needs to be implemented"
            }
            
            # Save report
            report_path = os.path.join(self.log_dir, f"monthly_report_{month_year}.json")
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            print(f"✅ Monthly report saved: {report_path}")
            
        except Exception as e:
            print(f"⚠️  Monthly report failed: {e}")

# =========================================================
# MOCK DATA CLASS FOR SIMULATION
# =========================================================

class MockZambianData:
    """Mock data collector for simulation/testing"""
    
    def fetch_all_sources(self):
        """Generate mock market data"""
        commodities = ["Maize", "Tomatoes", "Beans", "Rice", "Groundnuts", "Onions"]
        markets = ["Lusaka Central Market", "Kabwe Main Market", "Ndola Main Market", "Livingstone Market"]
        
        prices = []
        for commodity in commodities:
            for market in markets:
                base_price = {
                    "Maize": 120.50,
                    "Tomatoes": 85.25,
                    "Beans": 175.30,
                    "Rice": 200.00,
                    "Groundnuts": 190.00,
                    "Onions": 105.00
                }.get(commodity, 100.00)
                
                # Add random variation
                price = base_price * random.uniform(0.95, 1.05)
                
                prices.append({
                    "market": market,
                    "commodity": commodity,
                    "price": round(price, 2),
                    "unit": "ZMW/kg",
                    "volume": random.randint(1000, 5000),
                    "quality": random.choice(["Grade A", "Grade B", "Standard"]),
                    "source": "Mock_Data",
                    "verified": True,
                    "recorded_at": datetime.now().isoformat(),
                    "price_trend": random.choice(["up", "down", "stable"]),
                    "region": self._get_region_from_market(market)
                })
        
        print(f"📊 Generated {len(prices)} mock price records")
        return prices
    
    def _get_region_from_market(self, market):
        """Get region from market name"""
        if "Lusaka" in market:
            return "Lusaka"
        elif "Kabwe" in market:
            return "Central"
        elif "Ndola" in market:
            return "Copperbelt"
        elif "Livingstone" in market:
            return "Southern"
        else:
            return "Unknown"
    
    def fetch_znfu_prices(self):
        """Generate mock ZNFU prices"""
        # Return a subset with ZNFU as source
        all_prices = self.fetch_all_sources()
        for price in all_prices[:10]:
            price['source'] = 'ZNFU_Mock'
        return all_prices[:10]
    
    def fetch_maco_prices(self):
        """Generate mock MACO prices"""
        # Return a subset with MACO as source
        all_prices = self.fetch_all_sources()
        for price in all_prices[10:20]:
            price['source'] = 'MACO_Mock'
            price['region'] = self._get_region_from_market(price['market'])
        return all_prices[10:20]
    
    def generate_hourly_update(self):
        """Generate mock hourly update"""
        commodities = ["Maize", "Tomatoes"]
        markets = ["Lusaka Central Market", "Kabwe Main Market"]
        
        prices = []
        for commodity in commodities:
            for market in markets:
                base_price = {
                    "Maize": 120.50,
                    "Tomatoes": 85.25
                }.get(commodity, 100.00)
                
                # Small variation for hourly update
                variation = random.uniform(-0.02, 0.02)
                price = base_price * (1 + variation)
                
                prices.append({
                    "market": market,
                    "commodity": commodity,
                    "price": round(price, 2),
                    "unit": "ZMW/kg",
                    "source": "Mock_Hourly",
                    "verified": True,
                    "recorded_at": datetime.now().isoformat(),
                    "region": self._get_region_from_market(market)
                })
        
        return prices

# =========================================================
# DATABASE INITIALIZATION HELPER
# =========================================================

def initialize_database_schema():
    """Initialize database with correct schema"""
    print("🔧 Initializing database schema...")
    
    conn = sqlite3.connect('farm_market.db')
    cur = conn.cursor()
    
    # Create market_prices table with all required columns
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT NOT NULL,
            commodity TEXT NOT NULL,
            price REAL NOT NULL,
            unit TEXT DEFAULT 'ZMW/kg',
            volume REAL,
            quality TEXT,
            source TEXT,
            verified BOOLEAN DEFAULT 0,
            recorded_at TIMESTAMP,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            region TEXT,
            market_lat REAL,
            market_lon REAL,
            price_trend TEXT,
            UNIQUE(market, commodity, recorded_at)
        )
    """)
    
    # Create other required tables
    tables = [
        """CREATE TABLE IF NOT EXISTS admin_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT NOT NULL,
            type TEXT DEFAULT 'info',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            read_status INTEGER DEFAULT 0
        )""",
        
        """CREATE TABLE IF NOT EXISTS collection_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            operation TEXT,
            records_collected INTEGER,
            status TEXT,
            error_message TEXT,
            duration_seconds REAL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT,
            action TEXT,
            details TEXT,
            ip_address TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    ]
    
    for table_sql in tables:
        try:
            cur.execute(table_sql)
        except Exception as e:
            print(f"⚠️  Error creating table: {e}")
    
    conn.commit()
    conn.close()
    print("✅ Database schema initialized")

# =========================================================
# STANDALONE EXECUTION
# =========================================================

if __name__ == "__main__":
    print("=" * 70)
    print("📅 ZAMBIAN MARKET DATA SCHEDULER")
    print("=" * 70)
    
    # Initialize database schema first
    initialize_database_schema()
    
    # Create scheduler instance
    scheduler = DataScheduler()
    
    try:
        # Start the scheduler
        scheduler.start_scheduler()
        
        # Keep main thread alive
        print("\n🔄 Scheduler is running. Press Ctrl+C to stop.")
        print("Commands:")
        print("  - Type 'daily' to run daily collection now")
        print("  - Type 'hourly' to run hourly update now")
        print("  - Type 'backup' to create backup now")
        print("  - Type 'report' to generate daily report")
        print("  - Type 'summaries' to send SMS summaries")
        print("  - Type 'status' to show scheduler status")
        print("  - Type 'stop' to stop the scheduler")
        print("  - Type 'exit' to quit")
        
        while True:
            command = input("\nEnter command: ").strip().lower()
            
            if command == 'daily':
                scheduler.run_once_now("daily")
            elif command == 'hourly':
                scheduler.run_once_now("hourly")
            elif command == 'backup':
                scheduler.run_once_now("backup")
            elif command == 'report':
                scheduler.run_once_now("report")
            elif command == 'summaries':
                scheduler.run_once_now("summaries")
            elif command == 'status':
                print("📊 Scheduler Status:")
                print(f"   Running: {scheduler.running}")
                print(f"   Next jobs:")
                for job in schedule.get_jobs()[:5]:
                    print(f"     • {job}")
            elif command == 'stop':
                scheduler.stop_scheduler()
                break
            elif command == 'exit':
                scheduler.stop_scheduler()
                break
            else:
                print("❌ Unknown command. Try: daily, hourly, backup, report, summaries, status, stop, exit")
    
    except KeyboardInterrupt:
        print("\n\n👋 Shutting down scheduler...")
        scheduler.stop_scheduler()
    
    print("\n✅ Scheduler shutdown complete")
    print("=" * 70)