import aiohttp
import asyncio
from bs4 import BeautifulSoup
from functools import lru_cache
from deep_translator import GoogleTranslator
from pymongo import MongoClient
import urllib3
from concurrent.futures import ThreadPoolExecutor
import time
import random
from datetime import datetime
from calendar import monthrange


# Suppress SSL warnings (not recommended for production use)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES = 3
RETRY_DELAY = 2
TRANSLATION_BATCH_SIZE = 50
MAX_TRANSLATION_WORKERS = 10

MONGO_URL = os.getenv('MONGO_URL')
# MongoDB connection
client = MongoClient('MONGO_URL')
db = client['polls_database']
polls_collection = db['polls']
scraped_urls_collection = db['scraped_urls']

# Automatically get the current month and year
now = datetime.now()
year = now.year
month = now.month
_, num_days = monthrange(year, month)

# Generate URLs for all days in the current month
base_url = "https://www.indiabix.com/current-affairs/"
generated_urls = [f"{base_url}{year}-{month:02d}-{day:02d}/" for day in range(1, num_days + 1)]



async def get_html_from_url(url, session, retries=MAX_RETRIES):
    """Fetch HTML content from a URL."""
    for attempt in range(retries):
        try:
            async with session.get(url, ssl=False, timeout=30) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"Failed to retrieve URL after {retries} attempts: {url}. Error: {e}")
    return None

async def fetch_html_content(urls):
    """Fetch HTML content for multiple URLs concurrently."""
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        async def bounded_fetch(url):
            async with semaphore:
                return await get_html_from_url(url, session)
        return await asyncio.gather(*[bounded_fetch(url) for url in urls])
        
async def get_available_urls(session):
    """Extract valid URLs from the current affairs page."""
    html_content = await get_html_from_url(base_url, session)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = soup.find_all('a', class_='text-link me-3')

        # Extract URLs properly, handling both absolute and relative URLs
        available_urls = [
            link['href'].strip('/') if link['href'].startswith('http') 
            else base_url + link['href'].strip('/')
            for link in links if link.get('href')
        ]
        return available_urls
    return []





async def scrape_multiple_urls(urls):
    """Scrape HTML content and parse questions from the URLs."""
    html_results = await fetch_html_content(urls)

    all_questions = []
    for html_content, url in zip(html_results, urls):
        if html_content:
            questions = parse_questions(html_content)

            # Add date information from the URL
            try:
                date_part = url.rstrip('/').split('/')[-1]
                year, month, day = map(int, date_part.split('-'))
                for question in questions:
                    question['year'] = year
                    question['month'] = month
                    question['day'] = day
                all_questions.extend(questions)

                # Log the scraped URL to MongoDB
                log_scraped_url_to_mongodb(url)
            except ValueError as e:
                print(f"Error parsing date from URL {url}: {e}")

    return all_questions

def parse_questions(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    questions = []
    question_divs = soup.select("div.bix-div-container")
    
    for question_div in question_divs:
        question_data = {}
        question_text_div = question_div.select_one("div.bix-td-qtxt")
        question_data['question'] = question_text_div.get_text(strip=True) if question_text_div else 'Question not found'
        
        options = []
        option_divs = question_div.select("div.bix-td-option-val")
        if option_divs:
            options = [option_div.get_text(strip=True) for option_div in option_divs]
        question_data['options'] = options
        
        answer_div = question_div.select_one("div.bix-ans-option")
        if answer_div:
            correct_answer_span = answer_div.select_one("span.mdi")
            if correct_answer_span:
                class_to_answer = {
                    "mdi-alpha-a-circle-outline": "A",
                    "mdi-alpha-b-circle-outline": "B",
                    "mdi-alpha-c-circle-outline": "C",
                    "mdi-alpha-d-circle-outline": "D"
                }
                correct_answer = next(
                    (class_to_answer[cls] for cls in correct_answer_span.get("class", []) if cls in class_to_answer), 
                    "Unknown"
                )
                question_data['correct_answer'] = correct_answer
        
        explanation_div = question_div.select_one("div.bix-ans-description")
        question_data['explanation'] = explanation_div.get_text(strip=True) if explanation_div else 'No explanation provided'
        
        discuss_link = question_div.select_one("a.mdi-comment-text-outline")
        if discuss_link and discuss_link.has_attr('href'):
            discussion_url = discuss_link['href']
            category = discussion_url.split("/current-affairs/")[1].split("/")[0]
            question_data['category'] = category
        else:
            question_data['category'] = "Unknown"
        
        questions.append(question_data)
    
    return questions



@lru_cache(maxsize=1000)
def translate_text(text, target_language='gujarati'):
    translator = GoogleTranslator(source='auto', target=target_language)
    for attempt in range(MAX_RETRIES):
        try:
            return translator.translate(text)
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"Translation failed after {MAX_RETRIES} attempts: {e}")
                return text  # Return original text if all attempts fail

def translate_batch(texts, target_language):
    return [translate_text(text, target_language) for text in texts]

def translate_questions(questions, target_languages=('en', 'hi', 'gu')):
    with ThreadPoolExecutor(max_workers=MAX_TRANSLATION_WORKERS) as executor:
        for lang in target_languages:
            for i in range(0, len(questions), TRANSLATION_BATCH_SIZE):
                batch = questions[i:i+TRANSLATION_BATCH_SIZE]
                
                # Translate questions
                question_texts = [q['question'] for q in batch]
                translated_questions = list(executor.map(lambda t: translate_text(t, lang), question_texts))
                for q, tq in zip(batch, translated_questions):
                    q[f'question_{lang}'] = tq
                
                # Translate options
                for j, q in enumerate(batch):
                    q[f'options_{lang}'] = list(executor.map(lambda t: translate_text(t, lang), q['options']))
                
                # Translate explanations
                explanation_texts = [q['explanation'] for q in batch if q['explanation']]
                translated_explanations = list(executor.map(lambda t: translate_text(t, lang), explanation_texts))
                for q, te in zip((q for q in batch if q['explanation']), translated_explanations):
                    q[f'explanation_{lang}'] = te
                
                # Add a small delay between batches to avoid overwhelming the translation service
                time.sleep(random.uniform(1, 3))
    
    return questions

def generate_poll_data(questions):
    polls = []
    for idx, question_data in enumerate(questions, start=1):
        year = question_data['year']
        month = question_data['month']
        day = question_data['day']
        
        poll_id = f"poll_{str(year)[-2:]}_{month:02d}_{day:02d}_{idx:03d}"
        poll = {
            "_id": poll_id,
            "year": year,
            "month": month,
            "day": day,
            "category": question_data.get('category', 'General'),
            "correct_answers": {},
            "languages": {}
        }

        letter_to_index = {'A': 0, 'B': 1, 'C': 2, 'D': 3}
        
        for lang in ('en', 'hi', 'gu'):
            correct_answer_letter = question_data.get('correct_answer', "Unknown")
            correct_answer_idx = letter_to_index.get(correct_answer_letter, -1)

            if correct_answer_idx != -1:
                poll["correct_answers"][lang] = correct_answer_idx
            else:
                print(f"Warning: Invalid correct answer for question: {question_data['question']}")

            poll["languages"][lang] = {
                "question": question_data.get(f'question_{lang}', ""),
                "options": question_data.get(f'options_{lang}', []),
                "explanation": question_data.get(f'explanation_{lang}', "")
            }

        polls.append(poll)

    return polls


def save_poll_data_to_mongodb(polls):
    try:
        existing_poll_ids = set(
            poll["_id"] for poll in polls_collection.find({"_id": {"$in": [poll["_id"] for poll in polls]}}, {"_id": 1})
        )

        new_polls = [poll for poll in polls if poll["_id"] not in existing_poll_ids]

        if new_polls:
            result = polls_collection.insert_many(new_polls)
            print(f"Inserted {len(result.inserted_ids)} new polls into MongoDB.")
        else:
            print("No new polls to insert. All polls already exist in the database.")
    except Exception as e:
        print(f"Error saving polls to MongoDB: {e}")

async def scrape_multiple_urls(urls):
    html_results = await fetch_html_content(urls)

    all_questions = []
    for html_content, url in zip(html_results, urls):
        if html_content:
            questions = parse_questions(html_content)

            # Extract the correct year, month, and day from the URL
            try:
                # Split the URL correctly to extract the date
                date_part = url.rstrip('/').split('/')[-1]
                year, month, day = map(int, date_part.split('-'))
            except ValueError as e:
                print(f"Error parsing date from URL {url}: {e}")
                continue  # Skip this URL if there's an error

            # Add date information to each question
            for question in questions:
                question['year'] = year
                question['month'] = month
                question['day'] = day

            all_questions.extend(questions)

    return all_questions
    
def ensure_scraped_urls_collection():
    """Ensure the scraped_urls collection exists and has the correct index."""
    if 'scraped_urls' not in db.list_collection_names():
        print("Creating 'scraped_urls' collection...")
        db.create_collection('scraped_urls')
    else:
        print("'scraped_urls' collection already exists.")

    # Ensure a unique index on the 'url' field to prevent duplicates
    scraped_urls_collection.create_index("url", unique=True)


def get_unscraped_urls(available_urls):
    """Filter out already scraped URLs from MongoDB and log new URLs."""
    normalized_available_urls = [url.rstrip('/') for url in available_urls]

    try:
        # Fetch scraped URLs from MongoDB and normalize them
        scraped_urls = {entry['url'].rstrip('/') for entry in scraped_urls_collection.find({}, {"url": 1})}

        # Display scraped URLs for debugging
        print(f"Scraped URLs from MongoDB: {scraped_urls}")

        # Filter out already scraped URLs
        unscraped_urls = [url for url in normalized_available_urls if url not in scraped_urls]

        # Display URLs to be scraped
        print(f"New URLs to be scraped: {unscraped_urls}")

        # Log the new URLs to MongoDB
        if unscraped_urls:
            scraped_urls_collection.insert_many(
                [{"url": url, "scraped_at": datetime.now()} for url in unscraped_urls]
            )
            print(f"Logged {len(unscraped_urls)} new URLs to MongoDB.")

        return unscraped_urls
    except Exception as e:
        print(f"Error fetching or logging URLs in MongoDB: {e}")
        return normalized_available_urls  # In case of error, try to scrape all




async def main():
    # Automatically get the current month and year
    now = datetime.now()
    year = now.year
    month = now.month
    _, num_days = monthrange(year, month)

    # Generate URLs for all days in the current month without trailing slashes
    base_url = "https://www.indiabix.com/current-affairs/"
    generated_urls = [f"{base_url}{year}-{month:02d}-{day:02d}" for day in range(1, num_days + 1)]

    # Use a single session for all requests
    async with aiohttp.ClientSession() as session:
        print("Fetching available URLs from the website...")
        available_urls = await get_available_urls(session)

        if not available_urls:
            print("No available URLs found on the website.")
            return

        # Normalize both generated and available URLs (strip trailing slashes)
        available_urls = [url.rstrip('/') for url in available_urls]
        generated_urls = [url.rstrip('/') for url in generated_urls]

        # Display both generated and available URLs for troubleshooting
        print("\n--- Generated URLs ---")
        for url in generated_urls:
            print(url)

        print("\n--- Available URLs from Website ---")
        for url in available_urls:
            print(url)

        # Compare generated URLs with the ones available on the website
        valid_urls = [url for url in generated_urls if url in available_urls]

        if not valid_urls:
            print("\nNo matching URLs found between generated and available URLs.")
            return

        print("\n--- Matching URLs ---")
        for i, url in enumerate(valid_urls, 1):
            print(f"{i}. {url}")

        # Filter out already scraped URLs from MongoDB
        urls_to_scrape = get_unscraped_urls(valid_urls)

        if not urls_to_scrape:
            print("No new URLs to scrape.")
            return

        print(f"Scraping {len(urls_to_scrape)} new URLs...")

        # Run the asynchronous scraping and parsing
        all_scraped_questions = await scrape_multiple_urls(urls_to_scrape)

        if all_scraped_questions:
            print(f"Scraped {len(all_scraped_questions)} questions.")

            # Translate the questions into multiple languages
            translated_questions = translate_questions(all_scraped_questions)

            # Generate poll data from the translated questions
            poll_data = generate_poll_data(translated_questions)

            # Save the poll data to MongoDB
            save_poll_data_to_mongodb(poll_data)
        else:
            print("No questions found to scrape.")

# Run the async main function
asyncio.run(main())
