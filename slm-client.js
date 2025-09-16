import fetch from "node-fetch";
import "dotenv/config";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

console.log("OPENAI_API_KEY loaded?", OPENAI_API_KEY ? "Yes" : "No");

function extractJSON(raw) {
  const match = raw.match(/\{[\s\S]*\}/);
  if (!match) return null;
  try {
    return JSON.parse(match[0]);
  } catch {
    return null;
  }
}

export async function callSLM(prompt) {
  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [
          {
            role: "system",
            content:
              "You are a lead classification assistant. Always return response in strict JSON with keys: isRelevant, cleanCategory, summary.",
          },
          {
            role: "user",
            content: prompt,
          },
        ],
      }),
    });

    const data = await response.json();
    console.log("RAW:", JSON.stringify(data));

    const text = data?.choices?.[0]?.message?.content || "";

    if (!text) {
      return {
        isRelevant: false,
        cleanCategory: "",
        summary: "No text generated",
      };
    }

    //First try to parse direct JSON
    let parsed = extractJSON(text);
    if (parsed) return parsed;

    return {
      isRelevant: true,
      cleanCategory: "Uncategorized",
      summary: text.trim().slice(0, 500),
    };
  } catch (err) {
    console.error("SLM error:", err);
    return { isRelevant: false, cleanCategory: "", summary: "" };
  }
}
