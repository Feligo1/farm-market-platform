import os  # Add this line
import json
import boto3
from datetime import datetime
import schedule
import threading

class BackupManager:
    
    def __init__(self):
        self.s3_client = boto3.client('s3', 
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('AWS_SECRET_KEY')
        )
        self.bucket_name = "farmmarket-backups"
    
    def create_backup(self):
        """Create comprehensive backup"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Backup JSON files
        backup_data = {
            "prices": load_fallback(),
            "farmers": load_farmer_profiles(),
            "buyers": load_buyers(),
            "subscribers": load_sms_subscribers(),
            "timestamp": timestamp
        }
        
        # Save locally
        backup_file = f"backups/backup_{timestamp}.json"
        with open(backup_file, 'w') as f:
            json.dump(backup_data, f)
        
        # Upload to S3
        self.s3_client.upload_file(backup_file, self.bucket_name, f"backups/{timestamp}.json")
        
        # Database dump (MySQL)
        os.system(f"mysqldump -u root farmers_db > backups/db_{timestamp}.sql")
        
        print(f"Backup created: {timestamp}")
        return backup_file
    
    def restore_backup(self, backup_file):
        """Restore from backup file"""
        with open(backup_file, 'r') as f:
            backup_data = json.load(f)
        
        # Restore JSON files
        save_fallback(backup_data.get('prices', []))
        save_farmer_profiles(backup_data.get('farmers', []))
        save_buyers(backup_data.get('buyers', []))
        save_sms_subscribers(backup_data.get('subscribers', []))
        
        print(f"Restored from: {backup_file}")
    
    def schedule_backups(self):
        """Schedule automatic backups"""
        schedule.every().day.at("02:00").do(self.create_backup)
        schedule.every().sunday.at("03:00").do(self.create_full_backup)
        
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)
        
        thread = threading.Thread(target=run_scheduler, daemon=True)
        thread.start()