import re
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import pandas as pd
import time
import urllib.parse
import threading
import os  # Added to check if file exists

# Regular expression to match valid emails
email_regex = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.(com|org|net|edu|gov|mil)$"

# Function to clean up email (decode URL-encoded characters like %20)
def clean_email(email):
    # Decode URL-encoded characters (e.g., %20 to space)
    email = urllib.parse.unquote(email)
    # Remove leading/trailing spaces and handle unnecessary characters
    return email.strip()

# Function to extract emails from a URL
def extract_emails_from_url(url):
    emails = []
    status = None
    data_transfer_mb = 0.0
    elapsed_time = 0.0
    start_time = time.time()
    try:
        # Send a GET request with headers to request HTML content only
        response = requests.get(url, timeout=5, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'text/html'
        })
        status = response.status_code  # Get the HTTP status code

        # Calculate data transfer in MB
        data_transfer_mb = len(response.content) / (1024 * 1024)  # Convert bytes to MB

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Extract emails from `mailto:` links
            mailto_links = [clean_email(a['href'][7:]) for a in soup.find_all('a', href=True) if a['href'].startswith('mailto:')]
            # Extract emails from plain text using regex
            regex_emails = [clean_email(email) for email in re.findall(email_regex, soup.get_text())]
            # Combine both methods
            emails = list(set(mailto_links + regex_emails))
    except requests.exceptions.RequestException:
        status = 'Request Error'
    elapsed_time = time.time() - start_time
    return emails, status, data_transfer_mb, elapsed_time

# Function to process URLs in batches
def process_urls_in_batches(full_urls, base_url_map, batch_size, max_workers, output_csv):
    total_batches = (len(full_urls) + batch_size - 1) // batch_size  # Ceiling division
    print(f"Total number of batches to process: {total_batches}")

    # Global dictionary to hold emails per base URL
    global_base_url_emails = {}  # Map base URL to set of emails found across all batches
    processed_urls = set()  # To avoid processing the same URL multiple times

    for batch_num in range(total_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, len(full_urls))
        batch_urls = full_urls[batch_start:batch_end]
        batch_start_time = time.time()

        # Use ThreadPoolExecutor to process URLs in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(extract_emails_from_url, url): url for url in batch_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                if url in processed_urls:
                    continue  # Skip if already processed
                processed_urls.add(url)
                try:
                    emails, status, data_transfer_mb, elapsed_time = future.result()
                    base_url = base_url_map[url]
                    if base_url not in global_base_url_emails:
                        global_base_url_emails[base_url] = set()
                    global_base_url_emails[base_url].update(emails)
                except Exception as e:
                    pass  # Handle exceptions if necessary

        # Prepare data for output
        output_data = []
        for base_url, emails in global_base_url_emails.items():
            # Combine emails into a single string, separated by commas
            emails_str = ', '.join(sorted(emails))
            output_data.append({
                "URL": base_url,
                "Emails": emails_str
            })

        # Write the entire data to CSV after each batch
        output_df = pd.DataFrame(output_data)
        output_df.to_csv(output_csv, index=False)

        batch_end_time = time.time()
        batch_elapsed_time = batch_end_time - batch_start_time
        print(f"Batch {batch_num + 1}/{total_batches} processed {len(batch_urls)} URLs in {round(batch_elapsed_time, 2)} seconds.")

    print(f"Processing complete. Extracted emails are saved in {output_csv}.")

# Main function
if __name__ == "__main__":
    # Input CSV containing the list of base URLs
    input_csv = 'input/input_urls.csv'  # Replace with your input CSV path

    # Routes CSV containing common routes like /contact, /about, etc.
    routes_csv = 'input/routes.csv'  # Replace with your routes CSV path

    # Output CSV where results will be stored
    output_csv = 'output/extracted_emails.csv'

    # Batch size and max workers
    batch_size = 1000
    max_workers = 80

    # Read the input CSV file (containing base URLs)
    df = pd.read_csv(input_csv)
    base_urls = [url.strip() for url in df['URL'].tolist()]  # Assuming your CSV has a column named 'URL'

    # Read the routes CSV file
    routes_df = pd.read_csv(routes_csv)
    routes = [route.strip() for route in routes_df['Route'].tolist()]  # Assuming the CSV has a column named 'Route'

    # Generate all combinations of base URLs and routes
    full_urls = []
    base_url_map = {}  # Map each full URL to its base URL
    for base_url in base_urls:
        if not base_url.startswith('http://') and not base_url.startswith('https://'):
            base_url_with_protocol = 'http://' + base_url
        else:
            base_url_with_protocol = base_url
        base_url_with_protocol = base_url_with_protocol.rstrip('/')
        # Add base URL without any route
        full_urls.append(base_url_with_protocol)
        base_url_map[base_url_with_protocol] = base_url
        for route in routes:
            full_url = f"{base_url_with_protocol}/{route.lstrip('/')}"
            full_urls.append(full_url)
            base_url_map[full_url] = base_url

    # Remove duplicates
    full_urls = list(set(full_urls))

    # Remove existing output file if it exists
    if os.path.exists(output_csv):
        os.remove(output_csv)

    # Process URLs in batches and write results incrementally
    process_urls_in_batches(full_urls, base_url_map, batch_size, max_workers, output_csv)