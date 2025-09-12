import PuppeteerExtra from "puppeteer-extra";
import { Cluster } from "puppeteer-cluster";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import * as cheerio from "cheerio";
import xlsx from "xlsx";
import mongoose from "mongoose";
import readline from "readline";
import chalk from "chalk";
import winston from "winston";
import pLimit from "p-limit";
import { callSLM } from "./slm-client.js";
import "dotenv/config";


async function enrichWithSLM(item) {
  const prompt = `
Classify this lead in clean format:

Name: ${item.name}
Category: ${item.category}
Description: ${item.description}
Website: ${item.website || "N/A"}
Email: ${item.email || "N/A"}

Return ONLY valid JSON in this format:
{ "isRelevant": true/false, "cleanCategory": "string", "summary": "string" }
`;
  console.log("Prompt sent to SLM:", prompt);

  console.log("Sending to SLM:", item.name);  

  try {
    const response = await callSLM(prompt);
    console.log("SLM response:", response);  

    return { 
      ...item, 
      ...response,
      originalCategory: item.category,
      originalDescription: item.description
    };
  } catch (err) {
    console.error("SLM error:", err);
    return { 
      ...item, 
      isRelevant: false, 
      cleanCategory: item.category, 
      summary: item.description,
      originalCategory: item.category,
      originalDescription: item.description
    };
  }
}

const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      return `${timestamp} [${level.toUpperCase()}]: ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "scraper.log" }),
  ],
});

PuppeteerExtra.use(StealthPlugin());
const puppeteerExtra = PuppeteerExtra;


const getRandomDelay = (min, max) =>
  Math.floor(Math.random() * (max - min + 1)) + min;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const cleanPhoneNumber = (phone) => {
  if (!phone) return "";

  const cleaned = phone
    .replace(/Send to phone/g, "")
    .replace(/Send to phone/gi, "")
    .replace(/[^\d+]/g, "")
    .trim();

  return cleaned;
};

const retry = async (fn, retries, delayFn) => {
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (i === retries - 1) throw err;
      await sleep(delayFn());
    }
  }
};
const validateData = (item) => {
  const errors = [];
  if (!item.name || item.name.length < 2) errors.push("Invalid name");
  return errors;
};

const qualifyLead = (item) => {
  const errors = [];

  // Must have at least one contact (email OR phone)
  if (!item.email && !item.phone) errors.push("No contact info");

  // Website optional, but preferred
  // if (!item.website) errors.push("No website");

  if (item.rating < 0) errors.push("Low rating");

  // Allow small businesses with just 2+ reviews
  // if (parseInt(item.ratingCount) < 2) errors.push("Too few reviews");

  return errors;
};

const saveToExcel = (data, filePath) => {
  try {
    let wb;
    let ws;
    let existingData = [];

    // Check if file already exists
    try {
      wb = xlsx.readFile(filePath);
      ws = wb.Sheets[wb.SheetNames[0]];
      existingData = xlsx.utils.sheet_to_json(ws);
    } catch (err) {
      // File doesn't exist, create a new workbook
      wb = xlsx.utils.book_new();
    }

    const createBusinessId = (item) => {
      // Use name + phone + address to create a unique ID
      return `${item.name || ''}-${item.phone || ''}-${item.address || ''}`.toLowerCase().replace(/\s+/g, '');
    };

    // Create a Set of existing business IDs for quick lookup
    const existingBusinessIds = new Set();
    existingData.forEach(item => {
      const businessId = createBusinessId(item);
      existingBusinessIds.add(businessId);
    });

    // Filter out duplicates from new data
    const uniqueNewData = data.filter(item => {
      const businessId = createBusinessId(item);
      return !existingBusinessIds.has(businessId);
    });

    // Combine existing data with unique new data
    const allData = [...existingData, ...uniqueNewData];
    
    // Create worksheet with all data
    ws = xlsx.utils.json_to_sheet(allData);
    
    // If workbook is new, add the sheet to it
    if (wb.SheetNames.length === 0) {
      xlsx.utils.book_append_sheet(wb, ws, "BusinessData");
    } else {
      // Replace the first sheet with updated data
      wb.Sheets[wb.SheetNames[0]] = ws;
    }
    
    // Write to file
    xlsx.writeFile(wb, filePath);
    
    logger.info(chalk.green(`✓ Saved/Appended data to ${filePath}`));
    logger.info(chalk.blue(`✓ Added ${uniqueNewData.length} new records, skipped ${data.length - uniqueNewData.length} duplicates`));
  } catch (err) {
    logger.error(chalk.red(`Error saving to xlsx: ${err.message}`));
  }
};

const setupAPIInterception = async (page) => {
  page.googleMapAPIResponses = [];
  await page.evaluateOnNewDocument(() => {
    const XHR = XMLHttpRequest.prototype;
    const open = XHR.open;
    const send = XHR.send;

    XHR.open = function (method, url) {
      this.url = url;
      return open.apply(this, arguments);
    };

    XHR.send = function () {
      this.addEventListener("load", function () {
        if (
          this.url &&
          (this.url.includes("/maps/search") ||
            this.url.includes("/maps/place"))
        ) {
          try {
            if (!document.querySelector("#searchAPIResponseData")) {
              const element = document.createElement("div");
              element.id = "searchAPIResponseData";
              element.innerText = this.responseText;
              element.style.height = 0;
              element.style.overflow = "hidden";
              document.body.appendChild(element);
            } else {
              document.querySelector("#searchAPIResponseData").innerText =
                this.responseText;
            }
            console.log(
              "Intercepted Maps API data with length: " +
                this.responseText.length
            );
          } catch (err) {
            console.error("Error in XHR interception:", err);
          }
        }
      });
      return send.apply(this, arguments);
    };
  });

  page.on("response", async (response) => {
    const url = response.url();
    if (url.includes("/maps/search") || url.includes("/maps/place")) {
      try {
        const contentType = response.headers()["content-type"] || "";
        if (contentType.includes("json") || contentType.includes("text")) {
          const text = await response.text();
          if (text && text.length > 100) {
            page.googleMapAPIResponses.push({ url, data: text });
          }
        }
      } catch (e) {}
    }
  });
};

// MongoDB Setup
let mongoConnected = false;
let DataModel;

const connectToMongoDB = async () => {
  try {
    await mongoose.connect("mongodb://localhost/business_scraper");
    logger.info(chalk.green("✓ Connected to MongoDB"));

    const dataSchema = new mongoose.Schema({
      name: { type: String, required: true },
      phone: { type: String, default: null },
      rating: { type: Number, default: 0 },
      ratingCount: { type: String, default: "0" },
      address: { type: String, default: null },
      category: { type: String, default: null },
      website: { type: String, default: null },
      email: { type: String, default: null },
      description: { type: String, default: null },
      source: { type: String, default: "google_maps" },
    });

    DataModel = mongoose.model("Business", dataSchema);
    mongoConnected = true;
    return true;
  } catch (err) {
    logger.warn(chalk.yellow(`MongoDB connection failed: ${err.message}`));
    return false;
  }
};

// Anti-Detection Setup
const setupAntiDetection = async (page) => {
  await page.evaluateOnNewDocument(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    window.chrome = { runtime: {} };
    Object.defineProperty(navigator, "languages", {
      get: () => ["en-US", "en"],
    });
  });

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  );
  await page.setViewport({ width: 1920, height: 1080 });
  await page.setExtraHTTPHeaders({
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    Connection: "keep-alive",
  });
};

const checkForCaptcha = async (page) => {
  const captchaSelector =
    'form[action*="/sorry/index"], div[class*="captcha"], img[src*="/sorry/image"]';
  const hasCaptcha = await page.evaluate((selector) => {
    return !!document.querySelector(selector);
  }, captchaSelector);
  if (hasCaptcha) {
    logger.error(chalk.red("CAPTCHA detected. Saving screenshot..."));
    await page.screenshot({ path: "captcha_screenshot.png" });
    throw new Error("CAPTCHA detected");
  }
  return false;
};

const autoScrollGoogleMaps = async (page, maxResults) => {
  logger.info(chalk.cyan("Scrolling to load listings..."));

  let previousCount = 0;
  let retries = 0;

  while (true) {
    const currentCount = await page.evaluate(() => {
      return document.querySelectorAll('div[role="article"], div.Nv2PK').length;
    });

    logger.info(`Loaded so far: ${currentCount}`);

    if (currentCount >= maxResults) {
      logger.info(chalk.green(`✓ Reached ${currentCount}/${maxResults}`));
      break;
    }

    await page.evaluate(() => {
      const scrollContainer = document.querySelector("div.m6QErb[aria-label]");
      if (scrollContainer) {
        scrollContainer.scrollBy(0, scrollContainer.scrollHeight);
      }
    });
    await new Promise((resolve) =>
      setTimeout(resolve, 2000 + Math.random() * 2000)
    );

    if (currentCount === previousCount) {
      retries++;
    } else {
      retries = 0;
      previousCount = currentCount;
    }

    if (retries >= 3) {
      logger.info(chalk.yellow(`No more results. Total: ${currentCount}`));
      break;
    }
  }
};

const autoScrollYellowPages = async (page, maxResults) => {
  logger.info(chalk.cyan("Scrolling Yellow Pages to load all listings..."));

  const scrollResult = await page.evaluate(async (maxResults) => {
    return new Promise((resolve) => {
      let previousResultCount = 0;
      let noChangeCount = 0;
      let totalScrolls = 0;
      const maxScrollAttempts = 50;

      const countListings = () => {
        const selectors = [
          ".result",
          ".business-listing",
          ".srp-listing",
          ".v-card",
          '[data-ya-class="result"]',
          ".search-result",
        ];

        const allElements = new Set();
        selectors.forEach((selector) => {
          document
            .querySelectorAll(selector)
            .forEach((el) => allElements.add(el));
        });

        return allElements.size;
      };

      const setupShowMoreHandlers = () => {
        const buttonTexts = [
          "Show more",
          "Load more",
          "Next",
          "More results",
          "See more",
          "More",
          "Continue",
          "Load additional",
        ];

        const buttons = Array.from(
          document.querySelectorAll(
            "button, div[role='button'], span[role='button'], a"
          )
        ).filter((btn) => {
          const text = btn.textContent.toLowerCase();
          return buttonTexts.some((buttonText) =>
            text.includes(buttonText.toLowerCase())
          );
        });

        let clicked = false;
        buttons.forEach((btn) => {
          try {
            if (btn.offsetParent !== null) {
              btn.click();
              console.log(`Clicked button: ${btn.textContent.trim()}`);
              clicked = true;
              // Wait after clicking
              return new Promise((r) => setTimeout(r, 1000));
            }
          } catch (e) {}
        });

        return clicked;
      };

      const performScroll = () => {
        try {
          window.scrollBy(0, 1000);
          return true;
        } catch (e) {
          console.error("Error in performScroll:", e);
          return false;
        }
      };

      const scrollInterval = setInterval(async () => {
        const currentResults = countListings();
        console.log(
          `Scroll attempt ${
            totalScrolls + 1
          }: found ${currentResults} results (target: ${maxResults})`
        );

        const scrollSuccess = performScroll();
        const clickedButton = setupShowMoreHandlers();

        totalScrolls++;

        if (currentResults >= maxResults) {
          clearInterval(scrollInterval);
          console.log(
            `✓ Reached target: ${currentResults} listings found (target: ${maxResults})`
          );
          return resolve({
            resultsCount: currentResults,
            message: `Found ${currentResults} listings (reached target)`,
          });
        }

        if (!scrollSuccess && !clickedButton) {
          noChangeCount++;
          if (noChangeCount >= 3) {
            clearInterval(scrollInterval);
            console.log(
              `Scroll container no longer responsive, found ${currentResults} listings`
            );
            return resolve({
              resultsCount: currentResults,
              message: `Found ${currentResults} listings (scroll container unresponsive)`,
            });
          }
        }

        if (currentResults === previousResultCount) {
          noChangeCount++;
          if (noChangeCount >= 8) {
            console.log("Performing final scroll...");
            for (let i = 0; i < 5; i++) {
              performScroll();
              setupShowMoreHandlers();
              await new Promise((resolve) => setTimeout(resolve, 200));
            }

            setTimeout(() => {
              const finalResults = countListings();
              clearInterval(scrollInterval);
              console.log(
                `✓ Scrolling complete: ${finalResults} listings found (no more results loading)`
              );
              return resolve({
                resultsCount: finalResults,
                message: `Found ${finalResults} listings (no more results loading)`,
              });
            }, 2000);
            return;
          }
        } else {
          noChangeCount = 0;
          previousResultCount = currentResults;
        }

        if (totalScrolls >= maxScrollAttempts) {
          clearInterval(scrollInterval);
          console.log(
            `✓ Scrolling stopped after ${maxScrollAttempts} attempts, found ${currentResults} listings`
          );
          return resolve({
            resultsCount: currentResults,
            message: `Found ${currentResults} listings (max attempts reached)`,
          });
        }
      }, 1000);
    });
  }, maxResults);

  logger.info(
    chalk.green(
      `Yellow Pages scrolling complete: ${
        scrollResult?.message || "Unknown result"
      }`
    )
  );
  return scrollResult;
};

const extractRating = ($) => {
  const selectors = [
    'span[aria-label*="star rating"]',
    'div[class*="fontDisplay"]',
  ];
  for (const selector of selectors) {
    const text = $(selector).text().trim();
    const rating = Number.parseFloat(text);
    if (!isNaN(rating) && rating >= 0 && rating <= 5) return rating;
  }
  return 0;
};

const extractRatingCount = ($) => {
  const selectors = ['span[aria-label*="reviews"]', 'div:contains("reviews")'];
  for (const selector of selectors) {
    const text = $(selector).text().trim();
    const match = text.match(/\d+/);
    if (match) return match[0];
  }
  return "0";
};

const extractAddress = ($) => {
  const selectors = ['div:contains("Address")', 'div[class*="fontBodyMedium"]'];
  for (const selector of selectors) {
    const text = $(selector).text().trim();
    if (text && !text.includes("Directions")) return text;
  }
  return null;
};

const extractCategory = ($) => {
  const selectors = [
    'span:contains("Category")',
    'button[class*="fontBodyMedium"]',
  ];
  for (const selector of selectors) {
    const text = $(selector).text().trim();
    if (text) return text.split("·")[0].trim();
  }
  return null;
};

const extractDescription = async (page) => {
  try {
    const description = await page.evaluate(() => {
      const selectors = [
        'div[class*="fontBodyMedium"][class*="description"]',
        'div[class*="PYvSYb"]',
        'div[aria-label*="About"]',
        'div[class*="m6QErb"]',
        'div[data-section-id*="overview"]',
        'div[data-section-id*="description"]',
        'div[class*="section-description"]',
        'div[class*="business-description"]',
        'div[class*="about-business"]',
        'div[class*="overview-content"]',
        'div[itemprop="description"]',
        'meta[property="og:description"]',
        'meta[name="description"]',
      ];

      // Try to get description from meta tags first
      const metaDescription = document.querySelector(
        'meta[property="og:description"], meta[name="description"]'
      );
      if (
        metaDescription &&
        metaDescription.content &&
        metaDescription.content.length > 20
      ) {
        return metaDescription.content;
      }

      for (const selector of selectors) {
        const elements = document.querySelectorAll(selector);
        for (const element of elements) {
          if (element.offsetParent !== null) {
            // Only visible elements
            const text = element.textContent.trim();
            if (text && text.length > 30 && text.length < 500) {
              // Reasonable length for description
              return text;
            }
          }
        }
      }

      const aboutHeaders = document.querySelectorAll(
        "h2, h3, h4, div[aria-label]"
      );
      for (const header of aboutHeaders) {
        const headerText = header.textContent.toLowerCase();
        if (
          headerText.includes("about") ||
          headerText.includes("description") ||
          headerText.includes("overview")
        ) {
          const nextSibling = header.nextElementSibling;
          if (nextSibling && nextSibling.textContent.trim().length > 20) {
            return nextSibling.textContent.trim();
          }
        }
      }

      return null;
    });

    if (!description) return null;

    // Clean description
    const cleaned = description
      .replace(/[\n\r\t]+/g, " ")
      .replace(/\s+/g, " ") // Normalize spaces
      .replace(/[^\w\s.,!?\-&@#%$*()]/g, "")
      .trim()
      .substring(0, 300);

    return cleaned.length > 10 ? cleaned : null;
  } catch (err) {
    logger.warn(chalk.yellow(`Description extraction failed: ${err.message}`));
    return null;
  }
};

const extractEmailFromWebsite = async (page, website, businessName) => {
  if (!website) return null;

  try {
    logger.info(chalk.blue(`Extracting email from website: ${website}`));

    await page.setDefaultNavigationTimeout(30000);
    await page.setJavaScriptEnabled(true);
    // Add retry logic for navigation
    await retry(
      async () => {
        await page.goto(website, {
          waitUntil: "domcontentloaded",
          timeout: 30000,
        });
      },
      2,
      () => getRandomDelay(2000, 4000)
    );

    const emails = await page.evaluate((businessName) => {
      const results = [];
      const emailRegex = /([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+)/gi;
      const bodyText = document.body.innerText;
      const emailMatches = bodyText.match(emailRegex) || [];

      const uniqueEmails = new Set();
      emailMatches.forEach((email) => {
        if (email.length > 5 && email.includes("@") && email.includes(".")) {
          uniqueEmails.add(email.toLowerCase());
        }
      });

      uniqueEmails.forEach((email) => results.push(email));

      const mailtoLinks = document.querySelectorAll('a[href^="mailto:"]');
      mailtoLinks.forEach((link) => {
        const email = link.href.replace("mailto:", "").split("?")[0].trim();
        if (email && !uniqueEmails.has(email.toLowerCase())) {
          uniqueEmails.add(email.toLowerCase());
          results.push(email);
        }
      });

      return {
        emails: results.filter((email) => {
          const lowEmail = email.toLowerCase();
          const invalidPatterns = [
            "example.com",
            "yourdomain",
            "domain.com",
            "@example",
            "@test",
          ];
          return !invalidPatterns.some((pattern) => lowEmail.includes(pattern));
        }),
        contactUrl: null,
      };
    }, businessName);

    return emails.emails.length > 0 ? emails.emails[0] : null;
  } catch (err) {
    logger.warn(
      chalk.yellow(`Email extraction failed for ${website}: ${err.message}`)
    );
    return null;
  }
};

const parseGoogleMapsAPIData = async (page) => {
  try {
    if (
      !page.googleMapAPIResponses ||
      page.googleMapAPIResponses.length === 0
    ) {
      const hasAPIData = await page.evaluate(() => {
        return !!document.querySelector("#searchAPIResponseData");
      });

      if (!hasAPIData) {
        logger.info(chalk.yellow("No API responses captured to parse"));
        return null;
      }

      const apiData = await page.evaluate(() => {
        const element = document.querySelector("#searchAPIResponseData");
        if (!element) return null;

        const responseText = element.innerText;
        if (!responseText || responseText.length < 50) return null;

        console.log(
          "Found API data in searchAPIResponseData element:",
          responseText.substring(0, 100) + "..."
        );

        try {
          let cleanData = responseText;
          if (cleanData.startsWith(")]}'")) {
            cleanData = cleanData.substring(4);
          }

          if (
            cleanData.trim().startsWith("<!DOCTYPE") ||
            cleanData.trim().startsWith("<html") ||
            cleanData.includes("<body") ||
            cleanData.includes("<div")
          ) {
            console.log("Detected HTML response, skipping API parsing");
            return null;
          }

          const parsedData = JSON.parse(cleanData);
          let businessesData = null;

          if (parsedData) {
            if (parsedData.d) {
              try {
                const innerJson = JSON.parse(parsedData.d.substr(4));
                if (innerJson && innerJson[6] && innerJson[6][0]) {
                  businessesData = innerJson[6][0];
                } else if (innerJson && innerJson[64]) {
                  businessesData = innerJson[64];
                }
              } catch (e) {
                console.error("Error parsing inner JSON:", e);
              }
            }

            // Try multiple data paths
            if (!businessesData && parsedData[6] && parsedData[6][0]) {
              businessesData = parsedData[6][0];
            }

            if (!businessesData && parsedData[0]) {
              businessesData = parsedData[0];
            }

            if (!businessesData && parsedData.results) {
              businessesData = parsedData.results;
            }

            if (!businessesData && parsedData.features) {
              businessesData = parsedData.features;
            }
          }

          if (!businessesData || !Array.isArray(businessesData)) {
            console.log("No valid business data array found in API response");
            return null;
          }

          const results = [];
          const seenNames = new Set();

          for (const business of businessesData) {
            try {
              const name =
                business[14] || business.name || business.title || "";
              if (!name || seenNames.has(name)) continue;
              seenNames.add(name);

              const item = {
                name,
                address: business[39] || business[18] || business.address || "",
                category:
                  business[13] ||
                  business.category ||
                  business.types?.[0] ||
                  "",
                rating: business[4]?.[7] || business.rating || 0,
                ratingCount:
                  business[4]?.[8] || business.user_ratings_total || "0",
                phone: business[178]?.[0]?.[0] || business.phone || "",
                website: business[7]?.[0] || business.website || "",
                email: "",
                description: business[3]?.[1] || business.description || "", // Add description from API
                detailsNeeded: true,
                detailUrl: business[5]?.[0] || business.url || "",
              };

              results.push(item);
            } catch (e) {
              console.error("Error processing business:", e);
            }
          }

          console.log(`Extracted ${results.length} businesses from API data`);
          return results;
        } catch (err) {
          console.error("Error parsing API data:", err);
          return null;
        }
      });

      return apiData;
    }

    const responses = page.googleMapAPIResponses;
    const results = [];
    const seenNames = new Set();

    for (const response of responses) {
      try {
        let data = response.data;

        // Strip off )]}'
        if (data.startsWith(")]}'")) {
          data = data.substring(4);
        }

        if (
          data.trim().startsWith("<!DOCTYPE") ||
          data.trim().startsWith("<html") ||
          data.includes("<body") ||
          data.includes("<div") ||
          data.includes("<script")
        ) {
          console.log("Got HTML instead of JSON, skipping response...");
          continue;
        }

        let parsed;
        try {
          parsed = JSON.parse(data);
        } catch (e) {
          console.log("Not valid JSON, skipping API response...");
          continue;
        }

        let businesses = null;

        if (parsed.d) {
          try {
            const innerJson = JSON.parse(parsed.d.substr(4));
            if (innerJson && innerJson[6] && innerJson[6][0]) {
              businesses = innerJson[6][0];
            } else if (innerJson && innerJson[64]) {
              businesses = innerJson[64];
            }
          } catch (e) {
            console.error("Error parsing inner JSON:", e);
          }
        }

        if (!businesses && parsed[6] && parsed[6][0]) {
          businesses = parsed[6][0];
        }

        if (!businesses && parsed[0]) {
          businesses = parsed[0];
        }

        if (!businesses || !Array.isArray(businesses)) continue;

        for (const business of businesses) {
          try {
            const name = business[14] || business.name || business.title || "";
            if (!name || seenNames.has(name)) continue;
            seenNames.add(name);

            const item = {
              name,
              address: business[39] || business[18] || business.address || "",
              category:
                business[13] || business.category || business.types?.[0] || "",
              rating: business[4]?.[7] || business.rating || 0,
              ratingCount:
                business[4]?.[8] || business.user_ratings_total || "0",
              phone: business[178]?.[0]?.[0] || business.phone || "",
              website: business[7]?.[0] || business.website || "",
              email: "",
              description: "",
              detailsNeeded: true,
              detailUrl: business[5]?.[0] || business.url || "",
            };

            results.push(item);
          } catch (e) {
            console.error("Error processing business:", e);
          }
        }
      } catch (err) {
        console.error("Error parsing response:", err);
      }
    }

    logger.info(chalk.green(`Extracted ${results.length} items from API data`));
    return results.length > 0 ? results : null;
  } catch (err) {
    logger.warn(chalk.yellow(`API parsing failed: ${err.message}`));
    return null;
  }
};

const extractYellowPagesData = async (page) => {
  try {
    const content = await page.content();
    const $ = cheerio.load(content);

    const listings = [];
    const listingSelectors = [
      ".result",
      ".business-listing",
      ".srp-listing",
      ".v-card",
      '[data-ya-class="result"]',
      ".search-result",
    ];

    for (const selector of listingSelectors) {
      $(selector).each((i, el) => {
        try {
          const name = $(el)
            .find(".business-name, .name, [class*='name'], h2, h3, h4")
            .first()
            .text()
            .trim();
          if (!name || name.length < 2) return;

          const phone = $(el)
            .find('.phones, [class*="phone"], .phone')
            .first()
            .text()
            .trim();
          const address = $(el)
            .find('.adr, [class*="address"], .address, .street-address')
            .first()
            .text()
            .trim();

          let category = $(el)
            .find('.categories, [class*="category"], .category')
            .first()
            .text()
            .trim();
          if (!category) {
            category = $(el).find(".links a").first().text().trim();
          }

          const website = $(el)
            .find('.track-visit-website, .website, [href*="http"]')
            .first()
            .attr("href");

          // Extract description from Yellow Pages
          const description = $(el)
            .find(
              '.snippet, .description, [class*="desc"], .business-description'
            )
            .first()
            .text()
            .trim()
            .substring(0, 200); // Limit to 200 characters

          // Try to extract rating
          let rating = 0;
          let ratingCount = "0";
          const ratingElement = $(el)
            .find('.rating, [class*="star"], [aria-label*="star"]')
            .first();
          if (ratingElement.length) {
            const ratingText =
              ratingElement.attr("aria-label") || ratingElement.text();
            const ratingMatch = ratingText.match(/([0-9.]+)/);
            if (ratingMatch) rating = Number.parseFloat(ratingMatch[1]);

            const reviewText = $(el)
              .find('.count, .review-count, [class*="review"]')
              .text();
            const countMatch = reviewText.match(/(\d+)/);
            if (countMatch) ratingCount = countMatch[1];
          }

          listings.push({
            name,
            phone,
            address,
            category,
            rating,
            ratingCount,
            website: website && website.startsWith("http") ? website : null,
            email: "",
            description: description || "", // Add description here
            detailsNeeded: true,
            detailUrl: $(el).find("a").first().attr("href") || "",
            source: "yellow_pages",
          });
        } catch (err) {
          logger.warn(
            chalk.yellow(
              `Error processing Yellow Pages listing: ${err.message}`
            )
          );
        }
      });

      if (listings.length > 0) break;
    }

    logger.info(
      chalk.green(`Extracted ${listings.length} items from Yellow Pages`)
    );
    return listings;
  } catch (err) {
    logger.error(chalk.red(`Yellow Pages extraction failed: ${err.message}`));
    return [];
  }
};
const extractEmailFromYellowPages = async (page, detailUrl) => {
  try {
    if (!detailUrl.startsWith("http")) {
      detailUrl = `https://www.yellowpages.com${detailUrl}`;
    }

    await page.goto(detailUrl, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });

    await sleep(getRandomDelay(2000, 4000));

    const email = await page.evaluate(() => {
      const mailtoLinks = document.querySelectorAll('a[href^="mailto:"]');
      for (const link of mailtoLinks) {
        const email = link.href.replace("mailto:", "").split("?")[0].trim();
        if (email.includes("@") && !email.includes("example.com")) {
          return email;
        }
      }

      const bodyText = document.body.textContent;
      const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
      const emails = bodyText.match(emailRegex);

      if (emails) {
        for (const email of emails) {
          if (!email.includes("example.com") && !email.includes("domain.com")) {
            return email;
          }
        }
      }

      return null;
    });

    return email;
  } catch (err) {
    logger.warn(
      chalk.yellow(`Yellow Pages email extraction failed: ${err.message}`)
    );
    return null;
  }
};

const scrapeYellowPagesDetail = async (browser, item) => {
  if (!item.detailUrl || !item.detailsNeeded) return item;

  let page;
  try {
    page = await browser.newPage();
    await setupAntiDetection(page);

    if (!item.detailUrl.startsWith("http")) {
      item.detailUrl = `https://www.yellowpages.com${item.detailUrl}`;
    }

    await page.goto(item.detailUrl, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });

    await sleep(getRandomDelay(2000, 4000));

    item.email = await extractEmailFromYellowPages(page, item.detailUrl);

    if (!item.description || item.description.length < 20) {
      const additionalDescription = await page.evaluate(() => {
        const descriptionSelectors = [
          ".business-description",
          ".description-content",
          ".about-business",
          ".snippet",
          '[itemprop="description"]',
          ".additional-info",
          ".details-content",
        ];

        for (const selector of descriptionSelectors) {
          const element = document.querySelector(selector);
          if (element && element.textContent.trim().length > 20) {
            return element.textContent.trim().substring(0, 300);
          }
        }
        return null;
      });

      if (additionalDescription) {
        item.description = additionalDescription;
      }
    }

    item.detailsNeeded = false;
    return item;
  } catch (err) {
    logger.warn(
      chalk.yellow(`Yellow Pages detail scraping failed: ${err.message}`)
    );
    return item;
  } finally {
    if (page && !page.isClosed()) {
      await page.close();
    }
  }
};
// Main scraping function for Google Maps
const scrapeGoogleMaps = async (browser, query, location, maxResults) => {
  const page = await browser.newPage();
  await setupAntiDetection(page);
  await setupAPIInterception(page);
  await page.setDefaultNavigationTimeout(60000);

  try {
    const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(
      query
    )}+in+${encodeURIComponent(location)}`;
    logger.info(chalk.blue(`Navigating to: ${searchUrl}`));

    await page.goto(searchUrl, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    await sleep(getRandomDelay(3000, 5000));

    const hasCaptcha = await checkForCaptcha(page);
    if (hasCaptcha) {
      throw new Error("CAPTCHA detected");
    }

    await autoScrollGoogleMaps(page, maxResults);

    const apiData = await parseGoogleMapsAPIData(page);
    if (apiData && apiData.length > 0) {
      logger.info(
        chalk.green(
          `Successfully extracted ${apiData.length} items from Google Maps API`
        )
      );
      await page.close();
      return apiData;
    }

    logger.info(
      chalk.yellow("API data extraction failed, falling back to DOM parsing...")
    );

    const domData = await page.evaluate(() => {
      const results = [];
      const seenNames = new Set();

      const listings = document.querySelectorAll(
        'div[data-result-index], div.Nv2PK, div[jsaction*="mouseover"], a[data-result-index]'
      );

      console.log(`Found ${listings.length} potential listings in DOM`);

      // Function to extract phone number from element
      const extractPhoneNumberFromElement = (element) => {
        try {
          const phoneButton = element.querySelector(
            'button[data-item-id^="phone:tel:"]'
          );
          if (phoneButton) {
            const phoneData = phoneButton.getAttribute("data-item-id");
            if (phoneData && phoneData.startsWith("phone:tel:")) {
              return phoneData.replace("phone:tel:", "");
            }

            const phoneTextElement = phoneButton.querySelector(".Io6YTe");
            if (phoneTextElement) {
              return phoneTextElement.textContent.trim();
            }

            return phoneButton.textContent.trim();
          }

          const phoneButtonByAria = element.querySelector(
            'button[aria-label^="Phone:"]'
          );
          if (phoneButtonByAria) {
            const ariaLabel = phoneButtonByAria.getAttribute("aria-label");
            return ariaLabel.replace("Phone:", "").trim();
          }

          const phoneSelectors = [
            'button[data-item-id="phone"]',
            'button[aria-label*="phone"]',
            'span[aria-label*="phone"]',
            'a[href^="tel:"]',
            "div[data-phone-number]",
          ];

          for (const selector of phoneSelectors) {
            const phoneElement = element.querySelector(selector);
            if (phoneElement) {
              const rawPhone =
                phoneElement.getAttribute("aria-label") ||
                phoneElement.getAttribute("data-phone-number") ||
                phoneElement.getAttribute("href")?.replace("tel:", "") ||
                phoneElement.textContent;

              if (rawPhone && rawPhone.trim()) {
                return rawPhone.trim();
              }
            }
          }

          return "";
        } catch (err) {
          console.error("Error extracting phone number:", err);
          return "";
        }
      };

      // Function to extract website from element
      const extractWebsiteFromElement = (element) => {
        try {
          // METHOD 1: Using data-item-id attribute
          const websiteElement = element.querySelector(
            'a[data-item-id="authority"]'
          );
          if (websiteElement) {
            return websiteElement.getAttribute("href") || "";
          }

          // METHOD 2: Using aria-label
          const websiteByAria = element.querySelector(
            'a[aria-label*="website"]'
          );
          if (websiteByAria) {
            return websiteByAria.getAttribute("href") || "";
          }

          // METHOD 3: Fallback selectors
          const websiteSelectors = [
            'a[href^="http"]:not([href*="google.com"])',
            "a.lcr4fd",
          ];

          for (const selector of websiteSelectors) {
            const websiteElement = element.querySelector(selector);
            if (websiteElement) {
              const href = websiteElement.getAttribute("href");
              if (href && href.startsWith("http")) {
                return href;
              }
            }
          }

          return "";
        } catch (err) {
          console.error("Error extracting website:", err);
          return "";
        }
      };

      listings.forEach((element, index) => {
        try {
          const nameElement =
            element.querySelector("div[role='button'] div.fontHeadlineSmall") ||
            element.querySelector(
              "a[href*='/maps/place'] div.fontHeadlineSmall"
            ) ||
            element.querySelector("div.qBF1Pd.fontHeadlineSmall") ||
            element.querySelector("a[data-value='Title']") ||
            element.querySelector("div.fontHeadlineSmall") ||
            element.querySelector("h3") ||
            element.querySelector("a[href*='/maps/place']");

          let name = "";
          if (nameElement) {
            name =
              nameElement.textContent?.trim() ||
              nameElement.getAttribute("aria-label")?.trim() ||
              "";
          }

          if (!name || seenNames.has(name)) return;
          seenNames.add(name);

          const categoryElement =
            element.querySelector("div.W4Efsd:nth-of-type(1) span") ||
            element.querySelector("span.YhemCb") ||
            element.querySelector("div.W4Efsd") ||
            element.querySelector("span[jsinstance]");
          const category = categoryElement
            ? categoryElement.textContent.trim()
            : "";

          const addressElement =
            element.querySelector("div.W4Efsd:nth-of-type(2) span") ||
            element.querySelector("div.W4Efsd span[jsan]") ||
            element.querySelector("div.W4Efsd span[aria-hidden='true']") ||
            element.querySelector("span.UsdlK");
          const address = addressElement
            ? addressElement.textContent.trim()
            : "";

          const ratingElement =
            element.querySelector("span.MW4etd") ||
            element.querySelector("div.fontBodyMedium span") ||
            element.querySelector("span[aria-label*='star']");
          const rating = ratingElement
            ? Number.parseFloat(ratingElement.textContent) || 0
            : 0;

          const ratingCountElement =
            element.querySelector("span.UY7F9") ||
            element.querySelector("a[href*='reviews'] span") ||
            element.querySelector("button span");
          const ratingCount = ratingCountElement
            ? ratingCountElement.textContent.replace(/\D/g, "") || "0"
            : "0";

          // Use the improved extraction functions
          const phone = extractPhoneNumberFromElement(element);
          const website = extractWebsiteFromElement(element);

          const detailUrlElement =
            element.querySelector("a.hfpxzc") ||
            element.querySelector("a[href*='/maps/place']") ||
            element.querySelector("a[data-result-index]");
          const detailUrl = detailUrlElement ? detailUrlElement.href || "" : "";

          console.log(`Extracted business ${index + 1}: ${name}`);

          results.push({
            name,
            category,
            address,
            rating,
            ratingCount,
            phone,
            website,
            email: "",
            description: "",
            detailsNeeded: true,
            detailUrl,
            source: "google_maps",
          });
        } catch (err) {
          console.error("Error parsing DOM listing:", err);
        }
      });

      console.log(
        `Successfully extracted ${results.length} businesses from DOM`
      );
      return results;
    });

    logger.info(
      chalk.green(`Extracted ${domData.length} items from Google Maps DOM`)
    );
    await page.close();
    return domData;
  } catch (err) {
    logger.error(chalk.red(`Google Maps scraping failed: ${err.message}`));
    await page.close();
    throw err;
  }
};
const clickToRevealContactInfo = async (page) => {
  try {
    // Try to click phone button to reveal phone number
    const phoneButton =
      (await page.$('button[data-item-id="phone"]')) ||
      (await page.$('button[aria-label*="phone"]'));

    if (phoneButton) {
      await phoneButton.click();
      await sleep(1500); // Wait for info to load

      // Sometimes the actual number is in a different element after clicking
      // Look for the revealed phone number
      const revealedPhone = await page.evaluate(() => {
        const phoneElement =
          document.querySelector('span[aria-label*="phone"]') ||
          document.querySelector("div[data-phone-number]");
        return phoneElement ? phoneElement.textContent.trim() : null;
      });

      if (revealedPhone) {
        // Store the revealed phone number in page context for later extraction
        await page.evaluate((phone) => {
          window.revealedPhoneNumber = phone;
        }, revealedPhone);
      }
    }

    // Try to click website button to reveal website
    const websiteButton =
      (await page.$('a[data-item-id="authority"]')) ||
      (await page.$('a[aria-label*="website"]'));

    if (websiteButton) {
      await websiteButton.click();
      await sleep(1500); // Wait for info to load
    }
  } catch (err) {
    logger.warn(
      chalk.yellow(`Click to reveal contact info failed: ${err.message}`)
    );
  }
};
const extractContactInfoWithoutClicking = async (page) => {
  try {
    const contactInfo = await page.evaluate(() => {
      const result = { phone: "", website: "" };

      // EXTRACT PHONE NUMBER USING DATA ATTRIBUTES (YOUR DISCOVERY)
      // Method 1: Using data-item-id attribute (most reliable)
      const phoneButton = document.querySelector(
        'button[data-item-id^="phone:tel:"]'
      );
      if (phoneButton) {
        // Get from data attribute (cleanest)
        const phoneData = phoneButton.getAttribute("data-item-id");
        if (phoneData) {
          result.phone = phoneData.replace("phone:tel:", "");
        }

        // Fallback to text content
        if (!result.phone) {
          const phoneTextElement = phoneButton.querySelector(".Io6YTe");
          if (phoneTextElement) {
            result.phone = phoneTextElement.textContent.trim();
          } else {
            result.phone = phoneButton.textContent.trim();
          }
        }
      }

      // Method 2: Using aria-label attribute (secondary reliable method)
      if (!result.phone) {
        const phoneButtonByAria = document.querySelector(
          'button[aria-label^="Phone:"]'
        );
        if (phoneButtonByAria) {
          const ariaLabel = phoneButtonByAria.getAttribute("aria-label");
          result.phone = ariaLabel.replace("Phone:", "").trim();
        }
      }

      // EXTRACT WEBSITE
      const websiteElement = document.querySelector(
        'a[data-item-id="authority"]'
      );
      if (websiteElement) {
        result.website = websiteElement.getAttribute("href") || "";
      }

      // Fallback for website
      if (!result.website) {
        const websiteByAria = document.querySelector(
          'a[aria-label*="website"]'
        );
        if (websiteByAria) {
          result.website = websiteByAria.getAttribute("href") || "";
        }
      }

      return result;
    });

    // Clean the extracted phone number
    if (contactInfo.phone) {
      contactInfo.phone = cleanPhoneNumber(contactInfo.phone);
    }

    return contactInfo;
  } catch (err) {
    logger.warn(
      chalk.yellow(`Contact extraction without clicking failed: ${err.message}`)
    );
    return { phone: "", website: "" };
  }
};

const extractGoogleMapsContactInfo = async (browser, item) => {
  if (!item.detailUrl || !item.detailsNeeded) return item;

  let page;
  try {
    page = await browser.newPage();
    await setupAntiDetection(page);
    await page.setDefaultNavigationTimeout(45000);
    await page.setDefaultTimeout(30000);

    await retry(
      async () => {
        await page.goto(item.detailUrl, {
          waitUntil: "domcontentloaded",
          timeout: 45000,
        });
      },
      2,
      () => getRandomDelay(2000, 4000)
    );

    await sleep(getRandomDelay(2000, 4000));

    // Extract contact info
    const contactInfo = await extractContactInfoWithoutClicking(page);
    if (contactInfo.phone && !item.phone) item.phone = contactInfo.phone;
    if (contactInfo.website && !item.website)
      item.website = contactInfo.website;

    // Extract address if missing
    if (!item.address) {
      const addressInfo = await page.evaluate(() => {
        const addressButton = document.querySelector(
          'button[data-item-id="address"]'
        );
        if (addressButton) {
          const addressElement = addressButton.querySelector(".Io6YTe");
          return addressElement ? addressElement.textContent.trim() : "";
        }
        return "";
      });
      if (addressInfo) item.address = addressInfo;
    }

    // EXTRACT DESCRIPTION HERE
    if (!item.description) {
      item.description = await extractDescription(page);
    }

    item.detailsNeeded = false;
    return item;
  } catch (err) {
    logger.warn(
      chalk.yellow(
        `Google Maps detail scraping failed for ${item.name}: ${err.message}`
      )
    );
    return item;
  } finally {
    if (page && !page.isClosed()) {
      await page.close();
    }
  }
};

const scrapeYellowPages = async (browser, query, location, maxResults) => {
  let allListings = [];
  let currentPage = 1;
  let hasMorePages = true;
  let consecutiveFailures = 0;

  while (
    allListings.length < maxResults &&
    hasMorePages &&
    consecutiveFailures < 3
  ) {
    const page = await browser.newPage();
    await setupAntiDetection(page);
    await page.setDefaultNavigationTimeout(60000);

    const searchUrl = `https://www.yellowpages.com/search?search_terms=${encodeURIComponent(
      query
    )}&geo_location_terms=${encodeURIComponent(location)}&page=${currentPage}`;

    logger.info(
      chalk.blue(
        `Navigating to Yellow Pages (Page ${currentPage}): ${searchUrl}`
      )
    );

    try {
      await page.goto(searchUrl, {
        waitUntil: "domcontentloaded", // Changed from networkidle2
        timeout: 60000,
      });

      await sleep(getRandomDelay(3000, 5000));

      // More flexible waiting for results
      try {
        await page.waitForSelector(
          ".result, .search-result, .business-listing",
          {
            timeout: 15000,
          }
        );
      } catch (waitError) {
        logger.warn(chalk.yellow(`No results found on page ${currentPage}`));
        consecutiveFailures++;
        continue;
      }

      // Extract listings
      const listings = await page.evaluate(() => {
        const results = [];
        const listingElements = document.querySelectorAll(
          ".result, .search-result, .business-listing"
        );

        listingElements.forEach((el) => {
          try {
            const nameElement = el.querySelector(
              ".business-name, .srp-business-title, .business-name span"
            );
            if (!nameElement) return;

            const name = nameElement.textContent.trim();
            if (!name) return;

            const phoneElement = el.querySelector(
              ".phones, .srp-phone, .phone"
            );
            const addressElement = el.querySelector(
              ".adr, .srp-address, .address"
            );
            const categoryElement = el.querySelector(
              ".categories, .srp-categories, .category"
            );

            // Extract website
            let website = "";
            const websiteSelectors = [
              "a.track-visit-website",
              "a.website-link",
              "a.business-website",
              'a[href*="http"]:not([href*="yellowpages.com"])',
            ];

            for (const selector of websiteSelectors) {
              const websiteEl = el.querySelector(selector);
              if (websiteEl && websiteEl.href) {
                const href = websiteEl.href;
                if (
                  !href.includes("yellowpages.com") &&
                  !href.includes("track")
                ) {
                  website = href.split("?")[0]; // Remove tracking parameters
                  break;
                }
              }
            }

            // Get detail URL
            const detailLink =
              el.querySelector("a.business-name") ||
              el.querySelector('a[href*="/mip/"]') ||
              el.querySelector('a[href^="/"]');
            const detailUrl = detailLink ? detailLink.getAttribute("href") : "";

            results.push({
              name: name,
              phone: phoneElement ? phoneElement.textContent.trim() : "",
              address: addressElement ? addressElement.textContent.trim() : "",
              category: categoryElement
                ? categoryElement.textContent.trim()
                : "",
              website: website,
              source: "yellow_pages",
              rating: "",
              ratingCount: "",
              email: "",
              description: "",
              detailsNeeded: true,
              detailUrl: detailUrl,
            });
          } catch (error) {
            console.error("Error parsing listing:", error);
          }
        });

        return results;
      });

      if (listings.length === 0) {
        logger.info(chalk.yellow(`No results found at page ${currentPage}`));
        consecutiveFailures++;
      } else {
        allListings = [...allListings, ...listings];
        logger.info(
          chalk.blue(
            `Extracted ${listings.length} items from page ${currentPage}`
          )
        );
        consecutiveFailures = 0; // Reset on success
      }

      // Check for next page
      const nextPageExists = await page.evaluate(() => {
        const nextButton = document.querySelector(
          'a.next, .next-page, .pagination-next, a[aria-label="Next page"]'
        );
        return (
          nextButton !== null &&
          !nextButton.disabled &&
          nextButton.offsetParent !== null
        );
      });

      hasMorePages = nextPageExists && allListings.length < maxResults;
    } catch (error) {
      logger.error(
        chalk.red(`Error scraping page ${currentPage}: ${error.message}`)
      );
      consecutiveFailures++;
    } finally {
      await page.close();
    }

    if (hasMorePages) {
      currentPage++;
      await sleep(getRandomDelay(4000, 7000));
    }
  }

  logger.info(
    chalk.green(`Extracted ${allListings.length} items from Yellow Pages`)
  );
  return allListings.slice(0, maxResults);
};

const main = async () => {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  const question = (query) =>
    new Promise((resolve) => rl.question(query, resolve));

  try {
    console.log(chalk.blue("=== Business Lead Scraper ==="));
    console.log(chalk.blue("Supports Google Maps and Yellow Pages"));

    const query = await question(
      chalk.yellow("Enter business type to search (e.g., 'restaurants'): ")
    );
    const location = await question(
      chalk.yellow("Enter location (e.g., 'New York'): ")
    );
    const maxResults = Number.parseInt(
      await question(
        chalk.yellow("Enter maximum number of results to collect (e.g., 50): ")
      )
    );

    const sources = await question(
      chalk.yellow(
        "Enter sources to scrape (1=Google Maps, 2=Yellow Pages, 3=Both): "
      )
    );

    let useGoogleMaps = false;
    let useYellowPages = false;

    if (
      sources.includes("1") ||
      sources.includes("3") ||
      sources.toLowerCase().includes("google")
    ) {
      useGoogleMaps = true;
    }
    if (
      sources.includes("2") ||
      sources.includes("3") ||
      sources.toLowerCase().includes("yellow")
    ) {
      useYellowPages = true;
    }

    if (!useGoogleMaps && !useYellowPages) {
      console.log(
        chalk.red("No valid sources selected. Defaulting to Google Maps.")
      );
      useGoogleMaps = true;
    }

    const shouldExtractEmails =
      (await question(
        chalk.yellow("Extract emails from websites? (y/N): ")
      )) === "y";

    const outputFileName = await question(
      chalk.yellow("Enter output filename (without extension): ")
    );

    console.log(chalk.cyan("\nStarting scraping process..."));

    // Connect to MongoDB
    await connectToMongoDB();

    // Launch browser
    const browser = await puppeteerExtra.launch({
      headless: false,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-site-isolation-trials",
        "--disable-blink-features=AutomationControlled",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      ],
      defaultViewport: null,
    });

    let allResults = [];

    // Scrape from selected sources FIRST
    if (useGoogleMaps) {
      try {
        console.log(chalk.cyan("\nScraping Google Maps..."));
        const googleResults = await scrapeGoogleMaps(
          browser,
          query,
          location,
          maxResults
        );
        allResults = [...allResults, ...googleResults];
      } catch (err) {
        console.log(chalk.red(`Google Maps scraping failed: ${err.message}`));
      }
    }

    if (useYellowPages) {
      try {
        console.log(chalk.cyan("\nScraping Yellow Pages..."));
        const yellowPagesResults = await scrapeYellowPages(
          browser,
          query,
          location,
          maxResults
        );
        allResults = [...allResults, ...yellowPagesResults];
      } catch (err) {
        console.log(chalk.red(`Yellow Pages scraping failed: ${err.message}`));
      }
    }

    console.log(chalk.green(`\nTotal ${allResults.length} leads collected`));

    // extract detailed contact information
    console.log(chalk.cyan("\nExtracting detailed contact information..."));

    const detailLimit = pLimit(2);

    // Create a new browser instance for detail extraction to avoid connection issues
    const detailBrowser = await puppeteerExtra.launch({
      headless: false,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-site-isolation-trials",
        "--disable-blink-features=AutomationControlled",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      ],
      defaultViewport: null,
    });

    try {
      const detailExtractionPromises = allResults.map((item, index) =>
        detailLimit(async () => {
          if (item.detailsNeeded && item.detailUrl) {
            console.log(
              chalk.blue(
                `Extracting details for ${item.name} (${index + 1}/${
                  allResults.length
                })`
              )
            );

            if (item.source === "google_maps") {
              return await extractGoogleMapsContactInfo(detailBrowser, item);
            } else if (item.source === "yellow_pages") {
              return await scrapeYellowPagesDetail(detailBrowser, item);
            }
            return item;
          }
          return item;
        })
      );

      allResults = await Promise.all(detailExtractionPromises);
    } finally {
      // Close the detail browser after extraction
      await detailBrowser.close();
    }

    if (shouldExtractEmails) {
      console.log(chalk.cyan("\nExtracting emails from websites..."));

      // Use puppeteer-cluster for better resource management
      const cluster = await Cluster.launch({
        concurrency: Cluster.CONCURRENCY_CONTEXT,
        maxConcurrency: 3,
        puppeteerOptions: {
          headless: false,
          args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-web-security",
            "--disable-features=IsolateOrigins,site-per-process",
            "--memory-growth-limit=2048",
            "--max-old-space-size=2048",
            "--disable-site-isolation-trials",
            "--disable-blink-features=AutomationControlled",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          ],
          defaultViewport: null,
        },
      });

      // Task for email extraction
      await cluster.task(async ({ page, data: { website, businessName } }) => {
        return await extractEmailFromWebsite(page, website, businessName);
      });

      try {
        for (let i = 0; i < allResults.length; i++) {
          const item = allResults[i];
          if (item.website && !item.email) {
            console.log(
              chalk.blue(
                `Extracting email for ${item.name} (${i + 1}/${
                  allResults.length
                })`
              )
            );

            // Queue email extraction with error handling
            try {
              item.email = await cluster.execute({
                website: item.website,
                businessName: item.name,
              });
            } catch (err) {
              logger.warn(
                chalk.yellow(
                  `Email extraction failed for ${item.website}: ${err.message}`
                )
              );
              item.email = null;
            }

            await sleep(getRandomDelay(1000, 3000));
          }
        }
      } finally {
        await cluster.idle();
        await cluster.close();
      }
    }
    
    // SLM Enrichment - Add this section
    console.log(chalk.cyan("\nEnriching data with SLM classification..."));
    
    const enrichedResults = [];
    for (let i = 0; i < allResults.length; i++) {
      const item = allResults[i];
      console.log(chalk.blue(`Enriching ${item.name} (${i + 1}/${allResults.length})`));
      
      try {
        const enriched = await enrichWithSLM(item);
        enrichedResults.push(enriched);
      } catch (err) {
        console.error(chalk.red(`SLM enrichment failed for ${item.name}: ${err.message}`));
        // Add item without SLM data if enrichment fails
        enrichedResults.push({
          ...item,
          isRelevant: false,
          cleanCategory: item.category,
          summary: item.description,
          originalCategory: item.category,
          originalDescription: item.description
        });
      }
      
      // Add delay to avoid rate limiting
      await sleep(getRandomDelay(1000, 3000));
    }

    // Save to db (with SLM fields)
    if (mongoConnected) {
      console.log(chalk.cyan("\nSaving data to MongoDB..."));
      for (const item of enrichedResults) {
        try {
          const errors = [...validateData(item), ...qualifyLead(item)];

          if (errors.length > 0) {
            logger.warn(
              chalk.yellow(`Skipping ${item.name}: ${errors.join(", ")}`)
            );
            continue;
          }

          await DataModel.findOneAndUpdate(
            { name: item.name, source: item.source },
            { $set: item },
            { upsert: true, new: true }
          );
        } catch (err) {
          logger.error(
            chalk.red(`Error saving ${item.name} to MongoDB: ${err.message}`)
          );
        }
      }
      console.log(chalk.green("✓ Qualified data saved to MongoDB"));
    }

    // Save to Excel
    const excelPath = `./${outputFileName || "business_leads"}.xlsx`;
    const qualifiedResults = enrichedResults.filter(
      (item) => qualifyLead(item).length === 0
    );

    console.log(
      chalk.green(
        `\n${qualifiedResults.length} qualified leads found out of ${enrichedResults.length}`
      )
    );

    saveToExcel(qualifiedResults, excelPath);

    console.log(chalk.green("\n✓ Scraping completed successfully!"));
    console.log(chalk.green(`✓ Results saved to: ${excelPath}`));
    if (mongoConnected) {
      console.log(chalk.green("✓ Data saved to MongoDB"));
    }
  } catch (err) {
    logger.error(chalk.red(`Fatal error: ${err.message}`));
    console.error(err);
  } finally {
    rl.close();
    process.exit(0);
  }
};

main().catch(console.error);