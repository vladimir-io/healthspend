import { CreateMLCEngine, MLCEngine } from '@mlc-ai/web-llm';

const MODEL_ID = "Llama-3.2-1B-Instruct-q4f16_1-MLC";

let engineCache: MLCEngine | null = null;
let initPromise: Promise<MLCEngine | null> | null = null;
let isGenerating = false;

export async function preloadEngine(onProgress?: (text: string) => void): Promise<MLCEngine | null> {
    if (engineCache) return engineCache;
    if (initPromise) return initPromise;

    if (!(navigator as any).gpu) {
        console.warn("WebGPU not supported. Defaulting to deterministic templates.");
        return null;
    }

    initPromise = (async () => {
        try {
            const engine = await CreateMLCEngine(MODEL_ID, {
                initProgressCallback: (progress) => {
                    if (onProgress) onProgress(progress.text);
                }
            });
            engineCache = engine;
            return engine;
        } catch (e) {
            console.error("WebLLM Allocation Failed:", e);
            return null;
        } finally {
            initPromise = null;
        }
    })();

    return initPromise;
}

export function isEngineReady(): boolean {
    return engineCache !== null;
}

export async function generateLetter(
    hospitalName: string,
    cptCode: string,
    price: number,
    description: string,
    timeoutMs: number = 8000
): Promise<string> {
    const formattedPrice = price.toFixed(2);

    const deterministicFallback = `To the Billing Department at ${hospitalName},

I am writing to formally dispute the charges on my recent medical bill concerning CPT Code ${cptCode} (${description}).

Your hospital's officially published Machine-Readable File (MRF) strictly lists the cash price for this procedure at $${formattedPrice}. I am demanding that my outstanding balance be immediately adjusted to match this legally mandated, publicly available cash price.

If this adjustment is not made, I will escalate this matter to the state attorney general and the relevant consumer financial protection bureaus for predatory billing practices. I expect a revised, finalized bill reflecting the $${formattedPrice} total within 15 days.

Govern yourselves accordingly.`;

    if (!engineCache || isGenerating) {
        return deterministicFallback;
    }

    const DISPUTE_RESOLUTION_TEMPLATE = `Write a short, firm and legally assertive formal medical debt settlement letter to ${hospitalName}. 
    I am being billed for CPT code ${cptCode} (${description}). 
    According to your published Machine-Readable File, your cash price is $${formattedPrice}. 
    I demand my bill be adjusted to this exact cash price immediately. 
    Keep it concise, legally authoritative, under 150 words. Do not include placeholders.`;

    isGenerating = true;

    try {
        const generationPromise = engineCache.chat.completions.create({
            messages: [{ role: "user", content: DISPUTE_RESOLUTION_TEMPLATE }],
            temperature: 0.1,
            max_tokens: 250,
        });

        const timeoutPromise = new Promise<null>((resolve) =>
            setTimeout(() => resolve(null), timeoutMs)
        );

        const result = await Promise.race([generationPromise, timeoutPromise]);

        if (result === null) {
            return deterministicFallback;
        }

        const output = result.choices[0].message.content;
        return output ? output.trim() : deterministicFallback;

    } catch (e) {
        console.error("WebLLM Generation Error:", e);
        return deterministicFallback;
    } finally {
        isGenerating = false;
    }
}

export async function unloadEngine() {
    if (engineCache) {
        await engineCache.unload();
        engineCache = null;
    }
}