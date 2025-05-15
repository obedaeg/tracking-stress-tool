#!/usr/bin/env python3
"""
Stress Testing Tool for Tracking Systems
Generates and posts impressions, clicks, and conversions to multiple hosts

Simulates multiple concurrent users sending traffic to tracking endpoints.
Stores all events and responses in a PostgreSQL database.
"""

import argparse
import json
import random
import time
import uuid
import signal
import threading
import sys
from datetime import datetime, timezone
import concurrent.futures
import requests
from typing import Dict, List, Any, Optional, Tuple, Union
from faker import Faker

# Database imports
try:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    from psycopg2.extras import Json
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Initialize Faker
fake = Faker()

# Global flags
stop_threads = False
db_pool = None

# HTTP Session management
http_sessions = {}
http_sessions_lock = threading.Lock()

# Configuration
HOSTS = [
    # "http://tracking1.example.com",
    # "http://tracking2.example.com",
    # "http://tracking3.example.com"
    'http://172.20.10.11:8000',
    'http://172.20.10.13:5000',
    'http://172.20.10.2:8000',
]

# Endpoints
IMPRESSION_ENDPOINT = "/api/events/impressions"
CLICK_ENDPOINT = "/api/events/click"
CONVERSION_ENDPOINT = "/api/events/conversions"

# Verbose debugging
DEBUG = True

# Probabilities
CLICK_PROBABILITY = 0.4  # 40% chance of a click after impression
CONVERSION_PROBABILITY = 0.3  # 30% chance of a conversion after click

# Mock data pools for random generation
US_STATES = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", 
             "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
             "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
             "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
             "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

SEARCH_KEYWORDS = [
    "running shoes", "athletic wear", "fitness equipment", "sports gear",
    "trail running", "gym accessories", "workout clothes", "hiking boots",
    "athletic performance", "sport nutrition", "weight training", "cardio equipment"
]

ADVERTISERS = [
    {"advertiser_id": "adv-789", "advertiser_name": "Nike Inc."},
    {"advertiser_id": "adv-790", "advertiser_name": "Adidas AG"},
    {"advertiser_id": "adv-791", "advertiser_name": "Under Armour"},
    {"advertiser_id": "adv-792", "advertiser_name": "Puma"},
    {"advertiser_id": "adv-793", "advertiser_name": "Reebok"},
    {"advertiser_id": "adv-794", "advertiser_name": "New Balance"},
    {"advertiser_id": "adv-795", "advertiser_name": "ASICS"},
    {"advertiser_id": "adv-796", "advertiser_name": "Skechers"},
    {"advertiser_id": "adv-797", "advertiser_name": "Brooks"},
    {"advertiser_id": "adv-798", "advertiser_name": "Fila"}
]

CAMPAIGNS = [
    {"campaign_id": "camp-456", "campaign_name": "Fall Collection 2023"},
    {"campaign_id": "camp-457", "campaign_name": "Running Shoes Sale"},
    {"campaign_id": "camp-458", "campaign_name": "Summer Sports Event"},
    {"campaign_id": "camp-459", "campaign_name": "Back to School"},
    {"campaign_id": "camp-460", "campaign_name": "Holiday Promotion"},
    {"campaign_id": "camp-461", "campaign_name": "Fitness Challenge"},
    {"campaign_id": "camp-462", "campaign_name": "New Year Resolution"},
    {"campaign_id": "camp-463", "campaign_name": "Athletic Performance"},
    {"campaign_id": "camp-464", "campaign_name": "Limited Edition Collection"},
    {"campaign_id": "camp-465", "campaign_name": "Spring Outdoor Activities"}
]

AD_FORMATS = [
    "banner_728x90", "banner_300x250", "banner_160x600", 
    "video_pre_roll", "video_mid_roll", "interstitial",
    "native_feed", "sidebar", "popup", "text_ad"
]

PRODUCTS = [
    {"product_id": "prod-135", "name": "Air Max Pro", "price": 59.99},
    {"product_id": "prod-136", "name": "Ultraboost", "price": 89.99},
    {"product_id": "prod-137", "name": "Performance Tee", "price": 24.99},
    {"product_id": "prod-138", "name": "Running Shorts", "price": 29.99},
    {"product_id": "prod-139", "name": "Athletic Socks", "price": 12.99},
    {"product_id": "prod-140", "name": "Fitness Tracker", "price": 129.99},
    {"product_id": "prod-141", "name": "Water Bottle", "price": 19.99},
    {"product_id": "prod-142", "name": "Gym Bag", "price": 39.99},
    {"product_id": "prod-143", "name": "Running Jacket", "price": 79.99},
    {"product_id": "prod-144", "name": "Training Shoes", "price": 69.99}
]

CONVERSION_TYPES = ["purchase", "signup", "download", "subscription", "lead"]


def generate_uuid() -> str:
    """Generate a random UUID string"""
    return str(uuid.uuid4())


def get_timestamp() -> str:
    """Get current ISO8601 timestamp with timezone"""
    return datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')




def generate_ad(position: int) -> Dict[str, Any]:
    """Generate a random ad with advertiser and campaign"""
    advertiser = random.choice(ADVERTISERS)
    campaign = random.choice(CAMPAIGNS)
    ad_id = f"ad-{random.randint(100, 999)}"
    ad_name = f"{advertiser['advertiser_name']} {random.choice(['Pro', 'Elite', 'Max', 'Ultra', 'Sport', 'Flex'])}"
    ad_text = f"New {ad_name} - {random.choice(['Limited Edition', 'Sale', 'Best Seller', 'New Arrival', 'Featured'])}"
    ad_link = f"https://example.com/{ad_id.lower()}"
    ad_format = random.choice(AD_FORMATS)
    
    return {
        "advertiser": advertiser,
        "campaign": campaign,
        "ad": {
            "ad_id": ad_id,
            "ad_name": ad_name,
            "ad_text": ad_text,
            "ad_link": ad_link,
            "ad_position": position,
            "ad_format": ad_format
        }
    }


def generate_impression() -> Dict[str, Any]:
    """Generate a random impression event"""
    impression_id = f"imp-{generate_uuid()}"
    user_ip = fake.ipv4()
    user_agent = fake.user_agent()
    timestamp = get_timestamp()
    state = random.choice(US_STATES)
    search_keywords = random.choice(SEARCH_KEYWORDS)
    session_id = f"session-{generate_uuid()[:8]}"
    
    # Generate between 4 and 10 ads
    num_ads = random.randint(4, 10)
    ads = [generate_ad(position+1) for position in range(num_ads)]
    
    return {
        "impression_id": impression_id,
        "user_ip": user_ip,
        "user_agent": user_agent,
        "timestamp": timestamp,
        "state": state,
        "search_keywords": search_keywords,
        "session_id": session_id,
        "ads": ads
    }


def generate_click(impression: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Generate a click event with 40% probability
    Returns None if no click is generated
    """
    if random.random() > CLICK_PROBABILITY:
        return None
    
    click_id = f"click-{generate_uuid()}"
    timestamp = get_timestamp()
    impression_id = impression["impression_id"]
    
    # Randomly select one of the ads to click
    clicked_ad_data = random.choice(impression["ads"])["ad"]
    
    # Time to click: 1-30 seconds after impression
    time_to_click = round(random.uniform(1, 30), 1)
    
    # Generate click coordinates
    x = random.randint(50, 500)
    y = random.randint(50, 800)
    normalized_x = round(random.uniform(0.1, 0.9), 2)
    normalized_y = round(random.uniform(0.1, 0.9), 2)
    
    return {
        "click_id": click_id,
        "impression_id": impression_id,
        "timestamp": timestamp,
        "clicked_ad": {
            "ad_id": clicked_ad_data["ad_id"],
            "ad_position": clicked_ad_data["ad_position"],
            "click_coordinates": {
                "x": x,
                "y": y,
                "normalized_x": normalized_x,
                "normalized_y": normalized_y
            },
            "time_to_click": time_to_click
        },
        "user_info": {
            "user_ip": impression["user_ip"],
            "state": impression["state"],
            "session_id": impression["session_id"]
        }
    }


def generate_conversion(click: Dict[str, Any], impression: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Generate a conversion event with 30% probability
    Returns None if no conversion is generated
    """
    if random.random() > CONVERSION_PROBABILITY:
        return None
    
    conversion_id = f"conv-{generate_uuid()}"
    click_id = click["click_id"]
    impression_id = impression["impression_id"]
    timestamp = get_timestamp()
    
    # Select random product
    product = random.choice(PRODUCTS)
    
    # Generate random quantity 1-3
    quantity = random.randint(1, 3)
    
    # Calculate total price
    unit_price = product["price"]
    total_price = round(unit_price * quantity, 2)
    
    # Time to convert: 60-3600 seconds (1-60 minutes) after click
    time_to_convert = random.randint(60, 3600)
    
    conversion_type = random.choice(CONVERSION_TYPES)
    
    return {
        "conversion_id": conversion_id,
        "click_id": click_id,
        "impression_id": impression_id,
        "timestamp": timestamp,
        "conversion_type": conversion_type,
        "conversion_value": total_price,
        "conversion_currency": "USD",
        "conversion_attributes": {
            "order_id": f"order-{generate_uuid()[:8]}",
            "items": [
                {
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price
                }
            ]
        },
        "attribution_info": {
            "time_to_convert": time_to_convert,
            "attribution_model": "last_click",
            "conversion_path": [
                {
                    "event_type": "impression",
                    "timestamp": impression["timestamp"]
                },
                {
                    "event_type": "click",
                    "timestamp": click["timestamp"]
                }
            ]
        },
        "user_info": {
            "user_ip": impression["user_ip"],
            "state": impression["state"],
            "session_id": impression["session_id"]
        }
    }


# Database configuration defaults
DB_CONFIG = {
    "dbname": "tracking_events",
    "user": "trackuser",
    "password": "trackpass",
    "host": "localhost",
    "port": 5432,
    "min_connections": 1,
    "max_connections": 10
}


class DatabaseManager:
    """
    Manages database connections and operations
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.pool = None
        
    def initialize(self) -> bool:
        """Initialize the database connection pool"""
        if not HAS_PSYCOPG2:
            print("WARNING: psycopg2 is not installed. Database functionality is disabled.")
            return False
            
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.config.get("min_connections", 1),
                maxconn=self.config.get("max_connections", 10),
                dbname=self.config.get("dbname", "tracking_events"),
                user=self.config.get("user", "trackuser"),
                password=self.config.get("password", "trackpass"),
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 5432)
            )
            
            # Test connection
            conn = self.get_connection()
            if conn:
                self.return_connection(conn)
                print(f"Successfully connected to PostgreSQL database '{self.config.get('dbname')}'")
                return True
            return False
        except psycopg2.Error as e:
            print(f"Failed to initialize database connection: {str(e)}")
            return False
    
    def get_connection(self):
        """Get a connection from the pool"""
        if self.pool:
            try:
                return self.pool.getconn()
            except psycopg2.pool.PoolError as e:
                print(f"Error getting database connection: {str(e)}")
        return None
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool:
            self.pool.putconn(conn)
    
    def close(self):
        """Close all connections in the pool"""
        if self.pool:
            self.pool.closeall()
            print("Database connections closed")


def store_event(event_id: str, event_type: str, event_data: Dict[str, Any], 
                host_responses: List[Tuple[str, bool, str]]) -> bool:
    """
    Store an event and its host responses in the database
    
    Args:
        event_id: The ID of the event (impression_id, click_id, or conversion_id)
        event_type: The type of event ('impression', 'click', or 'conversion')
        event_data: The full event data as a dictionary
        host_responses: List of tuples with host responses: (host, success, message)
        
    Returns:
        True if the event was successfully stored, False otherwise
    """
    global db_pool
    
    if not db_pool:
        return False
    
    conn = db_pool.get_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            # Convert host responses to a format suitable for JSONB
            responses_json = []
            for host, success, message in host_responses:
                responses_json.append({
                    "host": host,
                    "success": success,
                    "message": message,
                    "timestamp": get_timestamp()
                })
            
            # Insert the event into the database
            cursor.execute(
                """
                INSERT INTO events (event_id, event_type, full_event, host_responses)
                VALUES (%s, %s, %s, %s)
                """,
                (event_id, event_type, Json(event_data), Json(responses_json))
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Error storing event in database: {str(e)}")
        try:
            conn.rollback()
        except:
            pass
        return False
    finally:
        db_pool.return_connection(conn)


def get_http_session(thread_id: int) -> requests.Session:
    """
    Get or create a requests Session for the current thread
    Using a session allows connection pooling and reuse
    """
    global http_sessions
    
    # Create a thread-specific key
    key = f"thread-{thread_id}"
    
    with http_sessions_lock:
        if key not in http_sessions:
            # Create a new session with connection pooling
            session = requests.Session()
            
            # Configure the session with appropriate connection limits
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10,  # Number of connection pools
                pool_maxsize=10,      # Number of connections per pool
                max_retries=0,        # We'll handle retries manually
                pool_block=False      # Don't block if pool is full
            )
            
            # Mount the adapter for both HTTP and HTTPS
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            # Store the session
            http_sessions[key] = session
            
            if DEBUG:
                print(f"Created new HTTP session for {key}")
        
        return http_sessions[key]


def post_to_host(host: str, endpoint: str, data: Dict[str, Any], 
                 timeout_ms: int = 2000, thread_id: int = None,
                 retry_count: int = 2) -> Tuple[bool, str]:
    """
    Post event data to a host endpoint with timeout in milliseconds
    Returns (success, message)
    Uses connection pooling for better performance
    Includes retry logic with exponential backoff
    """
    url = f"{host}{endpoint}"
    
    # Get the thread's HTTP session
    session = get_http_session(thread_id if thread_id is not None else 0)
    
    # Convert milliseconds to seconds for requests library
    timeout_sec = timeout_ms / 1000.0
    
    if DEBUG:
        print(f"Sending request to: {url}")
        print(f"Headers: Content-Type: application/json")
        print(f"Timeout: {timeout_sec}s")
        print(f"Data: {json.dumps(data, indent=2)[:500]}...")  # Truncate for readability
    
    # Initialize retry mechanism
    attempts = 0
    max_attempts = retry_count + 1  # Initial attempt + retries
    
    while attempts < max_attempts:
        try:
            attempts += 1
            
            # Use the session for connection pooling
            response = session.post(
                url,
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=timeout_sec
            )
            
            if DEBUG and attempts > 1:
                print(f"Retry {attempts-1} succeeded for {url}")
            
            if DEBUG:
                print(f"Response status: {response.status_code}")
                try:
                    print(f"Response headers: {dict(response.headers)}")
                    resp_text = response.text[:500] + "..." if len(response.text) > 500 else response.text
                    print(f"Response body: {resp_text}")
                except:
                    print("Could not print response details")
            
            if response.status_code >= 200 and response.status_code < 300:
                return True, f"Posted to {url}: {response.status_code}"
            else:
                # Don't retry 4xx errors (client errors)
                if response.status_code >= 400 and response.status_code < 500:
                    return False, f"Failed posting to {url}: {response.status_code} - {response.text[:200]}"
                    
                # For 5xx errors, retry if we have attempts left
                if attempts < max_attempts:
                    backoff_time = min(0.1 * (2 ** (attempts - 1)), 2.0)  # Exponential backoff, max 2 seconds
                    if DEBUG:
                        print(f"Retrying in {backoff_time:.2f}s after server error: {response.status_code}")
                    time.sleep(backoff_time)
                    continue
                else:
                    return False, f"Failed posting to {url} after {attempts} attempts: {response.status_code}"
        
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as e:
            error_type = "Connection timeout" if isinstance(e, requests.exceptions.ConnectTimeout) else "Read timeout"
            
            # Retry timeouts with backoff
            if attempts < max_attempts:
                backoff_time = min(0.1 * (2 ** (attempts - 1)), 2.0)  # Exponential backoff, max 2 seconds
                if DEBUG:
                    print(f"Retrying in {backoff_time:.2f}s after {error_type}")
                time.sleep(backoff_time)
                continue
            else:
                return False, f"{error_type} posting to {url} after {attempts} attempts (timeout={timeout_ms}ms)"
                
        except requests.exceptions.ConnectionError as e:
            # Connection errors often mean the host is down or unreachable
            # In high-thread environments, it's best to not retry these immediately
            if attempts < max_attempts:
                backoff_time = min(0.2 * (2 ** (attempts - 1)), 5.0)  # Longer backoff for connection errors
                if DEBUG:
                    print(f"Retrying in {backoff_time:.2f}s after connection error")
                time.sleep(backoff_time)
                continue
            else:
                return False, f"Connection error posting to {url} after {attempts} attempts: {str(e)}"
                
        except requests.RequestException as e:
            # Other request exceptions
            if attempts < max_attempts:
                backoff_time = min(0.1 * (2 ** (attempts - 1)), 2.0)
                if DEBUG:
                    print(f"Retrying in {backoff_time:.2f}s after request exception")
                time.sleep(backoff_time)
                continue
            else:
                return False, f"Request error posting to {url} after {attempts} attempts: {str(e)}"
                
        except Exception as e:
            # Unexpected errors should probably not be retried
            return False, f"Unexpected error posting to {url}: {str(e)}"


def post_event_to_all_hosts(endpoint: str, data: Dict[str, Any], 
                           timeout_ms: int = 2000, 
                           thread_id: int = None) -> List[Tuple[str, bool, str]]:
    """
    Post event data to all hosts in parallel with timeout in milliseconds
    Returns list of (host, success, message)
    Uses connection pooling and retry logic
    """
    results = []
    if DEBUG:
        print(f"Posting to {len(HOSTS)} host(s) via endpoint: {endpoint}")
    
    # Use a thread pool with size matching the number of hosts
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(HOSTS)) as executor:
        future_to_host = {
            executor.submit(post_to_host, host, endpoint, data, timeout_ms, thread_id): host
            for host in HOSTS
        }
        
        for future in concurrent.futures.as_completed(future_to_host):
            host = future_to_host[future]
            try:
                success, message = future.result()
                results.append((host, success, message))
            except Exception as e:
                results.append((host, False, f"Thread pool exception: {str(e)}"))
    
    return results


def generate_and_post_events(
    num_impressions: int = None,
    delay: float = 0.0,
    timeout_ms: int = 500,
    dry_run: bool = False,
    thread_id: int = None,
    counts_lock: threading.Lock = None,
    shared_counts: Dict[str, Any] = None,
    use_database: bool = False,
    fault_tolerant: bool = True
) -> Dict[str, Any]:
    """
    Generate and post impressions (and resulting clicks/conversions)
    If num_impressions is None, runs until stop_threads is set to True
    Returns counts of events generated and posted
    """
    # Initialize local counts for aggregate statistics
    local_counts = {
        "impressions_generated": 0,
        "impressions_posted": 0,
        "clicks_generated": 0,
        "clicks_posted": 0,
        "conversions_generated": 0,
        "conversions_posted": 0,
        "per_host": {}  # Will store per-host statistics
    }
    
    # Initialize per-host statistics
    for host in HOSTS:
        local_counts["per_host"][host] = {
            "impressions_posted": 0,
            "clicks_posted": 0,
            "conversions_posted": 0
        }
    
    # For thread identification in logs
    thread_prefix = f"[Thread {thread_id}] " if thread_id is not None else ""
    
    i = 0
    while (num_impressions is None or i < num_impressions) and not stop_threads:
        if i > 0 and delay > 0:
            time.sleep(delay)
        
        # Generate impression
        impression = generate_impression()
        local_counts["impressions_generated"] += 1
        
        if thread_id is None or i % 10 == 0:  # Reduce log verbosity in multi-threaded mode
            print(f"\n{thread_prefix}Generating impression {i+1}{'' if num_impressions is None else '/'+str(num_impressions)} (ID: {impression['impression_id']})")
        
        if dry_run:
            if thread_id is None:  # Only print full JSON in single-threaded mode
                print(f"DRY RUN - Impression: {json.dumps(impression, indent=2)}")
                
            # Store in database even in dry-run mode if database is enabled
            if use_database:
                # Mock results for dry run
                mock_results = [(host, True, "DRY RUN - Not actually posted") for host in HOSTS]
                store_event(impression["impression_id"], "impression", impression, mock_results)
        else:
            # Post impression to all hosts
            results = post_event_to_all_hosts(IMPRESSION_ENDPOINT, impression, timeout_ms, thread_id)
            success_count = sum(1 for _, success, _ in results if success)
            local_counts["impressions_posted"] += success_count
            
            # If all posts failed and we're not in fault-tolerant mode, slow down
            if success_count == 0 and not fault_tolerant:
                # Add a small delay to prevent overwhelming the system if all hosts are down
                time.sleep(min(1.0, timeout_ms / 2000))
            
            # Record per-host results
            for host, success, _ in results:
                if success:
                    local_counts["per_host"][host]["impressions_posted"] += 1
            
            if thread_id is None:  # Reduce log verbosity in multi-threaded mode
                for host, success, message in results:
                    print(f"  {host} - {'✓' if success else '✗'} - {message}")
            
            # Store in database if enabled
            if use_database:
                if store_event(impression["impression_id"], "impression", impression, results):
                    if thread_id is None or i % 10 == 0:
                        print(f"  {thread_prefix}Stored impression in database (ID: {impression['impression_id']})")
        
        # Try to generate click
        click = generate_click(impression)
        if click:
            local_counts["clicks_generated"] += 1
            if thread_id is None or i % 10 == 0:
                print(f"  {thread_prefix}Generated click (ID: {click['click_id']})")
            
            if dry_run:
                if thread_id is None:  # Only print full JSON in single-threaded mode
                    print(f"  DRY RUN - Click: {json.dumps(click, indent=2)}")
                
                # Store in database even in dry-run mode if database is enabled
                if use_database:
                    # Mock results for dry run
                    mock_results = [(host, True, "DRY RUN - Not actually posted") for host in HOSTS]
                    store_event(click["click_id"], "click", click, mock_results)
            else:
                # Post click to all hosts
                results = post_event_to_all_hosts(CLICK_ENDPOINT, click, timeout_ms, thread_id)
                success_count = sum(1 for _, success, _ in results if success)
                local_counts["clicks_posted"] += success_count
                
                # If all posts failed and we're not in fault-tolerant mode, slow down
                if success_count == 0 and not fault_tolerant:
                    time.sleep(min(1.0, timeout_ms / 2000))
                
                # Record per-host results
                for host, success, _ in results:
                    if success:
                        local_counts["per_host"][host]["clicks_posted"] += 1
                
                if thread_id is None:  # Reduce log verbosity in multi-threaded mode
                    for host, success, message in results:
                        print(f"    {host} - {'✓' if success else '✗'} - {message}")
                        
                # Store in database if enabled
                if use_database:
                    if store_event(click["click_id"], "click", click, results):
                        if thread_id is None or i % 10 == 0:
                            print(f"    {thread_prefix}Stored click in database (ID: {click['click_id']})")
            
            # Try to generate conversion
            conversion = generate_conversion(click, impression)
            if conversion:
                local_counts["conversions_generated"] += 1
                if thread_id is None or i % 10 == 0:
                    print(f"    {thread_prefix}Generated conversion (ID: {conversion['conversion_id']})")
                
                if dry_run:
                    if thread_id is None:  # Only print full JSON in single-threaded mode
                        print(f"    DRY RUN - Conversion: {json.dumps(conversion, indent=2)}")
                    
                    # Store in database even in dry-run mode if database is enabled
                    if use_database:
                        # Mock results for dry run
                        mock_results = [(host, True, "DRY RUN - Not actually posted") for host in HOSTS]
                        store_event(conversion["conversion_id"], "conversion", conversion, mock_results)
                else:
                    # Post conversion to all hosts
                    results = post_event_to_all_hosts(CONVERSION_ENDPOINT, conversion, timeout_ms, thread_id)
                    success_count = sum(1 for _, success, _ in results if success)
                    local_counts["conversions_posted"] += success_count
                    
                    # If all posts failed and we're not in fault-tolerant mode, slow down
                    if success_count == 0 and not fault_tolerant:
                        time.sleep(min(1.0, timeout_ms / 2000))
                    
                    # Record per-host results
                    for host, success, _ in results:
                        if success:
                            local_counts["per_host"][host]["conversions_posted"] += 1
                    
                    if thread_id is None:  # Reduce log verbosity in multi-threaded mode
                        for host, success, message in results:
                            print(f"      {host} - {'✓' if success else '✗'} - {message}")
                    
                    # Store in database if enabled
                    if use_database:
                        if store_event(conversion["conversion_id"], "conversion", conversion, results):
                            if thread_id is None or i % 10 == 0:
                                print(f"      {thread_prefix}Stored conversion in database (ID: {conversion['conversion_id']})")
        
        i += 1
        
        # Update shared counts if running in multi-threaded mode
        if shared_counts is not None and counts_lock is not None:
            with counts_lock:
                for key in shared_counts:
                    if key == 'per_host':
                        # Special handling for the nested per_host dictionary
                        for host in shared_counts[key]:
                            if host in local_counts.get('per_host', {}):
                                for metric in shared_counts[key][host]:
                                    shared_counts[key][host][metric] += local_counts['per_host'][host].get(metric, 0)
                    elif isinstance(shared_counts[key], dict):
                        # Handle other dictionary types (in case we add more in the future)
                        local_dict = local_counts.get(key, {})
                        if isinstance(local_dict, dict):
                            for sub_key in local_dict:
                                if sub_key in shared_counts[key]:
                                    shared_counts[key][sub_key] += local_dict[sub_key]
                    else:
                        # For numeric counters, add the value
                        shared_counts[key] += local_counts.get(key, 0)
                
                # Reset local counts after adding to shared
                # Initialize a new dictionary with the same structure but zeroed values
                local_counts = {
                    "impressions_generated": 0,
                    "impressions_posted": 0,
                    "clicks_generated": 0,
                    "clicks_posted": 0,
                    "conversions_generated": 0,
                    "conversions_posted": 0,
                    "per_host": {}
                }
                
                # Re-initialize per-host statistics with zero values
                for host in HOSTS:
                    local_counts["per_host"][host] = {
                        "impressions_posted": 0,
                        "clicks_posted": 0,
                        "conversions_posted": 0
                    }
    
    return local_counts


def user_thread_func(thread_id: int, delay: float, timeout_ms: int, 
                     dry_run: bool, counts_lock: threading.Lock,
                     shared_counts: Dict[str, Any], use_database: bool = False,
                     fault_tolerant: bool = True) -> None:
    """Function executed by each user thread"""
    print(f"[Thread {thread_id}] Starting user simulation")
    
    # Initialize local_counts before try block to avoid UnboundLocalError
    local_counts = {
        "impressions_generated": 0,
        "impressions_posted": 0,
        "clicks_generated": 0,
        "clicks_posted": 0,
        "conversions_generated": 0,
        "conversions_posted": 0,
        "per_host": {}
    }
    
    # Initialize per-host statistics
    for host in HOSTS:
        local_counts["per_host"][host] = {
            "impressions_posted": 0,
            "clicks_posted": 0,
            "conversions_posted": 0
        }
    
    try:
        # Generate and post events
        local_counts = generate_and_post_events(
            num_impressions=None,  # Run indefinitely until stopped
            delay=delay,
            timeout_ms=timeout_ms,
            dry_run=dry_run,
            thread_id=thread_id,
            counts_lock=counts_lock,
            shared_counts=shared_counts,
            use_database=use_database,
            fault_tolerant=fault_tolerant
        )
        
        # Any code here will be skipped if generate_and_post_events raises an exception
        
    except Exception as e:
        print(f"[Thread {thread_id}] Error in thread: {str(e)}")
    finally:
        # Always execute this block, regardless of whether an exception occurred
        try:
            # Add any remaining counts to shared counts
            with counts_lock:
                for key in shared_counts:
                    if key == 'per_host':
                        # Special handling for the nested per_host dictionary
                        for host in shared_counts[key]:
                            if host in local_counts.get('per_host', {}):
                                for metric in shared_counts[key][host]:
                                    shared_counts[key][host][metric] += local_counts['per_host'][host].get(metric, 0)
                    elif isinstance(shared_counts[key], dict):
                        # Handle other dictionary types (in case we add more in the future)
                        local_dict = local_counts.get(key, {})
                        if isinstance(local_dict, dict):
                            for sub_key in local_dict:
                                if sub_key in shared_counts[key]:
                                    shared_counts[key][sub_key] += local_dict[sub_key]
                    else:
                        # For numeric counters, add the value
                        shared_counts[key] += local_counts.get(key, 0)
        except Exception as e:
            print(f"[Thread {thread_id}] Error updating shared counts: {str(e)}")
    
    print(f"[Thread {thread_id}] User simulation stopped")


def run_multi_threaded_test(num_threads: int, duration: int, delay: float, 
                           timeout_ms: int, dry_run: bool, 
                           use_database: bool = False,
                           fault_tolerant: bool = True) -> Dict[str, Any]:
    """
    Run a multi-threaded stress test with the specified number of user threads
    for the specified duration
    """
    global stop_threads
    stop_threads = False
    
    # Shared counts dictionary with lock for thread-safe updates
    shared_counts = {
        "impressions_generated": 0,
        "impressions_posted": 0,
        "clicks_generated": 0,
        "clicks_posted": 0,
        "conversions_generated": 0,
        "conversions_posted": 0,
        "per_host": {}
    }
    
    # Initialize per-host statistics
    for host in HOSTS:
        shared_counts["per_host"][host] = {
            "impressions_posted": 0,
            "clicks_posted": 0,
            "conversions_posted": 0
        }
    
    # Create a thread-safe lock for updating shared counts
    counts_lock = threading.Lock()
    
    print(f"Starting {num_threads} user threads with {timeout_ms}ms request timeout")
    print(f"Fault tolerance mode: {'Enabled' if fault_tolerant else 'Disabled'}")
    
    # Create and start user threads
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(
            target=user_thread_func,
            args=(i+1, delay, timeout_ms, dry_run, counts_lock, 
                  shared_counts, use_database, fault_tolerant)
        )
        thread.daemon = True  # Thread will exit when main program exits
        threads.append(thread)
        thread.start()
    
    try:
        # Print progress during the test
        start_time = time.time()
        while time.time() - start_time < duration:
            time.sleep(5)  # Update every 5 seconds
            elapsed = time.time() - start_time
            remaining = max(0, duration - elapsed)
            
            # Make a copy of the current counts to avoid lock contention
            with counts_lock:
                # Create a safe deep copy for reporting progress
                current_counts = {}
                for key, value in shared_counts.items():
                    if isinstance(value, dict):
                        current_counts[key] = value.copy()  # shallow copy is enough for our metrics
                    else:
                        current_counts[key] = value
            
            print(f"\nTest progress: {elapsed:.1f}s elapsed, {remaining:.1f}s remaining")
            print(f"Impressions: {current_counts['impressions_generated']}, " +
                  f"Clicks: {current_counts['clicks_generated']}, " +
                  f"Conversions: {current_counts['conversions_generated']}")
        
        # Signal threads to stop
        stop_threads = True
        print("\nTest duration completed, stopping user threads...")
        
        # Wait for all threads to finish (with timeout)
        for thread in threads:
            thread.join(timeout=5.0)
        
        return shared_counts
    
    except KeyboardInterrupt:
        print("\nTest interrupted by user, stopping...")
        stop_threads = True
        for thread in threads:
            thread.join(timeout=5.0)
        return shared_counts


def main():
    """Main function to parse arguments and run the stress test"""
    parser = argparse.ArgumentParser(description='Stress Testing Tool for Tracking Systems')
    parser.add_argument('--impressions', type=int, default=10,
                        help='Number of impressions to generate in single-threaded mode (default: 10)')
    parser.add_argument('--delay', type=float, default=1.0,
                        help='Delay between impression generations in seconds (default: 1.0)')
    parser.add_argument('--request-timeout', type=int, default=2000,
                        help='Timeout for HTTP requests in milliseconds (default: 2000)')
    parser.add_argument('--fault-tolerant', action='store_true',
                        help='Continue at full speed even when hosts are unreachable')
    parser.add_argument('--debug', action='store_true',
                        help='Enable verbose debug output')
    parser.add_argument('--impression-endpoint', type=str, default='/api/events/impressions',
                        help='API endpoint for impressions (default: /api/events/impressions)')
    parser.add_argument('--click-endpoint', type=str, default='/api/events/click',
                        help='API endpoint for clicks (default: /api/events/click)')
    parser.add_argument('--conversion-endpoint', type=str, default='/api/events/conversions',
                        help='API endpoint for conversions (default: /api/events/conversions)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Do not post events, just print them')
    parser.add_argument('--hosts', type=str,
                        help='Comma-separated list of hosts to use instead of defaults')
    parser.add_argument('--seed', type=int,
                        help='Random seed for reproducible data generation')
    parser.add_argument('--threads', type=int,
                        help='Number of concurrent user threads to simulate (default: none)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Duration of the stress test in seconds when using threads (default: 60)')
    
    # Database arguments
    parser.add_argument('--use-database', action='store_true',
                        help='Store events in PostgreSQL database')
    parser.add_argument('--db-host', type=str, default='localhost',
                        help='PostgreSQL hostname (default: localhost)')
    parser.add_argument('--db-port', type=int, default=5432,
                        help='PostgreSQL port (default: 5432)')
    parser.add_argument('--db-name', type=str, default='tracking_events',
                        help='PostgreSQL database name (default: tracking_events)')
    parser.add_argument('--db-user', type=str, default='trackuser',
                        help='PostgreSQL username (default: trackuser)')
    parser.add_argument('--db-password', type=str, default='trackpass',
                        help='PostgreSQL password (default: trackpass)')
    
    args = parser.parse_args()
    
    # Set random seed if specified
    if args.seed:
        random.seed(args.seed)
        fake.seed_instance(args.seed)
    
    # Enable debug mode if specified
    global DEBUG
    DEBUG = args.debug
    
    # Override default hosts if specified
    global HOSTS
    if args.hosts:
        HOSTS = [host.strip() for host in args.hosts.split(',')]
    
    # Override endpoints if specified
    global IMPRESSION_ENDPOINT, CLICK_ENDPOINT, CONVERSION_ENDPOINT
    IMPRESSION_ENDPOINT = args.impression_endpoint
    CLICK_ENDPOINT = args.click_endpoint
    CONVERSION_ENDPOINT = args.conversion_endpoint
    
    # Initialize database if requested
    global db_pool
    if args.use_database:
        if not HAS_PSYCOPG2:
            print("ERROR: psycopg2 is required for database functionality.")
            print("Please install it with: pip install psycopg2-binary")
            sys.exit(1)
            
        print("Initializing database connection...")
        DB_CONFIG.update({
            "host": args.db_host,
            "port": args.db_port,
            "dbname": args.db_name,
            "user": args.db_user,
            "password": args.db_password
        })
        
        db_pool = DatabaseManager(DB_CONFIG)
        if not db_pool.initialize():
            print("ERROR: Failed to initialize database connection. Exiting.")
            sys.exit(1)
    
    print(f"Stress Testing Tool for Tracking Systems")
    print(f"=======================================")
    
    # Register signal handler for clean shutdown
    def signal_handler(sig, frame):
        global stop_threads
        print("\nReceived interrupt signal, shutting down...")
        stop_threads = True
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Multi-threaded or single-threaded operation
    if args.threads:
        print(f"Multi-threaded mode with {args.threads} concurrent users")
        print(f"Test duration: {args.duration} seconds")
        print(f"Using hosts: {', '.join(HOSTS)}")
        print(f"{'DRY RUN MODE - ' if args.dry_run else ''}Delay: {args.delay}s, Request timeout: {args.request_timeout}ms")
        print(f"Debug mode: {'Enabled' if DEBUG else 'Disabled'}")
        print(f"Impression endpoint: {IMPRESSION_ENDPOINT}")
        print(f"Click endpoint: {CLICK_ENDPOINT}")
        print(f"Conversion endpoint: {CONVERSION_ENDPOINT}")
        print(f"Click probability: {CLICK_PROBABILITY*100}%")
        print(f"Conversion probability: {CONVERSION_PROBABILITY*100}%")
        print(f"Database storage: {'Enabled' if args.use_database else 'Disabled'}")
        print(f"=======================================")
        
        start_time = time.time()
        counts = run_multi_threaded_test(
            num_threads=args.threads,
            duration=args.duration,
            delay=args.delay,
            timeout_ms=args.request_timeout,
            dry_run=args.dry_run,
            use_database=args.use_database,
            fault_tolerant=args.fault_tolerant
        )
        end_time = time.time()
    else:
        print(f"Single-threaded mode generating {args.impressions} impressions")
        print(f"Using hosts: {', '.join(HOSTS)}")
        print(f"{'DRY RUN MODE - ' if args.dry_run else ''}Delay: {args.delay}s, Request timeout: {args.request_timeout}ms")
        print(f"Debug mode: {'Enabled' if DEBUG else 'Disabled'}")
        print(f"Impression endpoint: {IMPRESSION_ENDPOINT}")
        print(f"Click endpoint: {CLICK_ENDPOINT}")
        print(f"Conversion endpoint: {CONVERSION_ENDPOINT}")
        print(f"Click probability: {CLICK_PROBABILITY*100}%")
        print(f"Conversion probability: {CONVERSION_PROBABILITY*100}%")
        print(f"Database storage: {'Enabled' if args.use_database else 'Disabled'}")
        print(f"=======================================")
        
        start_time = time.time()
        counts = generate_and_post_events(
            num_impressions=args.impressions,
            delay=args.delay,
            timeout_ms=args.request_timeout,
            dry_run=args.dry_run,
            use_database=args.use_database
        )
        end_time = time.time()
    
    duration = end_time - start_time
    
    # Calculate percentages safely (avoid division by zero)
    click_percentage = (counts['clicks_generated']/max(1, counts['impressions_generated'])*100)
    conversion_percentage = (counts['conversions_generated']/max(1, counts['clicks_generated'])*100)
    
    print(f"\nFinal Summary:")
    print(f"=============")
    print(f"Test duration: {duration:.2f} seconds")
    print(f"Impressions generated: {counts['impressions_generated']}")
    print(f"Impressions posted: {counts['impressions_posted']} of {counts['impressions_generated']*len(HOSTS)} attempts")
    print(f"Clicks generated: {counts['clicks_generated']} ({click_percentage:.1f}% of impressions)")
    print(f"Clicks posted: {counts['clicks_posted']} of {counts['clicks_generated']*len(HOSTS)} attempts")
    print(f"Conversions generated: {counts['conversions_generated']} ({conversion_percentage:.1f}% of clicks)")
    print(f"Conversions posted: {counts['conversions_posted']} of {counts['conversions_generated']*len(HOSTS)} attempts")
    
    # Calculate rates per second
    print(f"\nRates:")
    print(f"======")
    print(f"Impressions per second: {counts['impressions_generated']/duration:.2f}")
    print(f"Clicks per second: {counts['clicks_generated']/duration:.2f}")
    print(f"Conversions per second: {counts['conversions_generated']/duration:.2f}")
    
    # Report per-host statistics
    print(f"\nPer-Host Statistics:")
    print(f"===================")
    
    # Calculate the maximum host name length for alignment
    max_host_length = max(len(host) for host in HOSTS)
    
    # Header row
    print(f"{'Host':{max_host_length}} | {'Impressions':12} | {'Clicks':12} | {'Conversions':12} | {'Success Rate':12}")
    print(f"{'-'*max_host_length} | {'-'*12} | {'-'*12} | {'-'*12} | {'-'*12}")
    
    # Data rows for each host
    for host in HOSTS:
        host_stats = counts['per_host'].get(host, {
            "impressions_posted": 0, 
            "clicks_posted": 0, 
            "conversions_posted": 0
        })
        
        # Calculate total events for this host
        total_expected = counts['impressions_generated'] + counts['clicks_generated'] + counts['conversions_generated']
        total_actual = (host_stats.get('impressions_posted', 0) + 
                       host_stats.get('clicks_posted', 0) + 
                       host_stats.get('conversions_posted', 0))
        
        # Calculate success rate for this host
        success_rate = (total_actual / max(1, total_expected)) * 100
        
        print(f"{host:{max_host_length}} | "
              f"{host_stats.get('impressions_posted', 0):12} | "
              f"{host_stats.get('clicks_posted', 0):12} | "
              f"{host_stats.get('conversions_posted', 0):12} | "
              f"{success_rate:10.1f}%")


    # Clean up resources
    if args.use_database and db_pool:
        db_pool.close()
    
    # Close all HTTP sessions
    with http_sessions_lock:
        for key, session in http_sessions.items():
            try:
                session.close()
                if DEBUG:
                    print(f"Closed HTTP session for {key}")
            except:
                pass


if __name__ == "__main__":
    main()