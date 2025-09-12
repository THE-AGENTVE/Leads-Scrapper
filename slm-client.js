import fetch from "node-fetch";
import "dotenv/config";

const HF_API_KEY =  process.env.HF_TOKEN;

console.log("HF_API_KEY loaded?", HF_API_KEY ? "✅ Yes" : "❌ No");

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
    const response = await fetch(
      "https://router.huggingface.co/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${HF_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          model: "mistralai/Mistral-7B-Instruct-v0.2:featherless-ai",
          messages: [
            {
              role: "user",
              content: prompt,
            },
          ],
        }),
      }
    );

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

    // ✅ First try to parse direct JSON
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