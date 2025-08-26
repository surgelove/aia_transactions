
import subprocess
import threading
from tkinter import messagebox
import pyttsx3
import pandas as pd
from datetime import datetime, timedelta
import pytz

def say_nonblocking(text, voice=None, volume=2):
    """
    Say text on macOS using the 'say' command in a non-blocking way 
    
    Parameters:
    -----------
    text : str
        Text to speak
    voice : str, optional
        Voice to use (e.g., 'Alex', 'Samantha', 'Victoria')
    volume : int, optional
        Volume level (0-100, default is 50)
    """
    print("Speaking:", text)
    def speak():
        try:
            # Set system volume before speaking
            # This requires 'osascript' which is available on macOS
            volume_cmd = ['osascript', '-e', f'set volume output volume {volume}']
            subprocess.run(volume_cmd, check=True)
            
            # Now speak the text
            cmd = ['say']
            if voice:
                cmd.extend(['-v', voice])
            cmd.append(text)
            subprocess.run(cmd, check=True)
            
            # Optional: Reset volume to a default level when done
            # subprocess.run(['osascript', '-e', 'set volume output volume 75'], check=True)
        except Exception as e:
            print(f"Error with text-to-speech: {e}")
    
    # Run in a separate thread to avoid blocking
    thread = threading.Thread(target=speak, daemon=True)
    thread.start()

def say_hello():
    print("Hello")
    messagebox.showinfo("Greeting", "Hello")
    engine = pyttsx3.init()
    engine.say("Hello")
    engine.runAndWait()

def convert_utc_to_ny(utc_time_str):
    """Convert UTC timestamp string to New York timezone formatted string"""
    try:
        return (datetime.fromisoformat(utc_time_str.replace('Z', '+00:00'))
                .astimezone(pytz.timezone('America/New_York'))
                .replace(tzinfo=None, microsecond=0)
                .strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        print(f"Error converting time: {e}")
        return None

def updown(direction):
    return "up" if direction > 0 else "down" if direction < 0 else None

class TimeBasedMovement:

    def __init__(self, range):
        self.data = []  # Fixed: added 'self.' prefix
        self.range = range
        self.max_size = 500

    def add(self, timestamp, price):
        self.data.append(
            {
                "timestamp": timestamp,
                "price": price,
            }
        )
        
        # Remove oldest data if queue exceeds max size
        if len(self.data) > self.max_size:
            self.data.pop(0)

    def clear(self):
        """Clear the stored data."""
        self.data = []

    def calc(self):
        # Calculate the movement of the price for the last 5 minutes
        if len(self.data) < 2:
            return 0.0

        # Get the price data for the last n minutes
        range_ago = self.data[-1]["timestamp"] - pd.Timedelta(minutes=self.range)
        relevant_data = [d for d in self.data if d["timestamp"] > range_ago]

        if not relevant_data:
            return 0.0

        # Calculate the price movement percentage
        start_price = relevant_data[0]["price"]
        end_price = relevant_data[-1]["price"]
        
        # Avoid division by zero
        if start_price == 0:
            return 0.0
            
        return ((end_price - start_price) / start_price) * 100


