import aiohttp
import asyncio
import json
import logging

API_URL = 'https://www.onemap.gov.sg/api/common/elastic/search'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('postal_code_fetcher.log'),
        logging.StreamHandler()
    ]
)

async def fetch_first_address_for_postal_code(session, postal_code, semaphore):
    async with semaphore:
        logging.info(f'Fetching postal code: {postal_code}')
        while True:
            params = {
                'searchVal': postal_code,
                'returnGeom': 'Y',
                'getAddrDetails': 'Y',
                'pageNum': 1
            }
            try:
                async with session.get(API_URL, params=params, timeout=10) as response:
                    if response.status != 200:
                        logging.warning(f'Failed to fetch {postal_code}, status code: {response.status}')
                        break
                    data = await response.json()
                    results = data.get('results', [])
                    if not results:
                        logging.info(f'No results for postal code: {postal_code}')
                        break
                    result = results[0]
                    return result
            except asyncio.TimeoutError:
                logging.error(f'Timeout error fetching postal code: {postal_code}')
                break
            except aiohttp.ClientError as e:
                logging.error(f'Client error: {e} for postal code: {postal_code}')
                break

async def main():
    postal_sectors = [
        '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
        '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
        '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
        '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
        '41', '42', '43', '44', '45', '46', '47', '48', '49', '50',
        '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
        '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
        '71', '72', '73', '75', '76', '77', '78', '79', '80'
    ]
    postal_codes = []

    logging.info("Generating postal codes...")
    for sector in postal_sectors:
        for i in range(10000):
            postal_codes.append(f"{sector}{i:04}")

    logging.info(f"Total postal codes generated: {len(postal_codes)}")

    postal_code_data = {}

    semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_first_address_for_postal_code(session, postal_code, semaphore) for postal_code in postal_codes]
        results = await asyncio.gather(*tasks)

    for result in results:
        if result:
            postal_code = result.get('POSTAL')
            if postal_code:
                postal_code_data[postal_code] = result

    with open('postal_codes_data.json', 'w', encoding='utf-8') as f:
        json.dump(postal_code_data, f, ensure_ascii=False, indent=4)

    logging.info(f"Total postal codes found: {len(postal_code_data)}")
    logging.info("Data has been saved to 'postal_codes_data.json'.")

if __name__ == '__main__':
    asyncio.run(main())
