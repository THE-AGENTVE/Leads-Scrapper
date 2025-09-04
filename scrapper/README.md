# Business Lead Scraper

This project scrapes business leads from Google Maps and Yellow Pages, with options to extract emails and save results to Excel.

## Prerequisites

- [Node.js](https://nodejs.org/) (v18+ recommended)
- [MongoDB](https://www.mongodb.com/try/download/community) (optional, for data persistence)

## Setup

1. **Install dependencies:**
   ```sh
   npm install
   ```

2. **Install Puppeteer Chrome browser:**
   ```sh
   npx puppeteer browsers install chrome
   ```

3. **(Optional) Start MongoDB:**
   ```sh
   mongod
   ```

## Running the Scraper

Run the main script:
```sh
node scrapper.js
```

Follow the prompts to:
- Enter business type (e.g., `restaurants`)
- Enter location (e.g., `New York`)
- Enter max results (e.g., `50`)
- Choose sources (Google Maps, Yellow Pages, or both)
- Choose whether to extract emails
- Enter output filename (without extension)

## Output

Results are saved as an Excel file (`.xlsx`) in the project directory.

## Notes

- If you see errors about Chrome not found, run the Puppeteer browser install command above.
- If MongoDB is not running, data will not be saved to the database, but Excel export will still work.
- Make sure your `.gitignore` excludes `node_modules`, logs, and output files.

## Troubleshooting

- **MongoDB connection failed:** Start MongoDB or check your connection settings.
- **Chrome not found:** Run `npx puppeteer browsers install chrome`.
- **CAPTCHA or scraping issues:** Try reducing the number of results or use a VPN/proxy.