# save as check_env.py
import os
from dotenv import load_dotenv

load_dotenv()

print("🌍 Environment Check:")
print("-" * 40)
print(f"AFRICASTALKING_USERNAME: {repr(os.getenv('AFRICASTALKING_USERNAME'))}")
print(f"AFRICASTALKING_API_KEY: {repr(os.getenv('AFRICASTALKING_API_KEY'))}")
print(f".env file exists: {os.path.exists('.env')}")
print(f"Current dir: {os.getcwd()}")