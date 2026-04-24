import { CreateMLCEngine, MLCEngine } from '@mlc-ai/web-llm';

const MODEL_ID = "Llama-3.2-1B-Instruct-q4f16_1-MLC";

let engineCache: MLCEngine | null = null;
let initPromise: Promise<MLCEngine | null> | null = null;
let isGenerating = false;

export async function preloadEngine(onProgress?: (text: string) => void): Promise<MLCEngine | null> {
    if (engineCache) return engineCache;
    if (initPromise) return initPromise;

    if (!(navigator as any).gpu) return null;

    initPromise = (async () => {
        try {
            const engine = await CreateMLCEngine(MODEL_ID, {
                initProgressCallback: (progress) => {
                    if (onProgress) onProgress(progress.text);
                }
            });
            engineCache = engine;
            return engine;
        } catch {
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

export type ClaimLetterIntent = 'price_shopping' | 'bill_above_posted';

export async function generateLetter(
    hospitalName: string,
    cptCode: string,
    price: number,
    description: string,
    timeoutMs: number = 8000,
    intent: ClaimLetterIntent = 'price_shopping'
): Promise<string> {
    const formattedPrice = price.toFixed(2);

    const deterministicShopping = `To the Billing and Patient Financial Services team at ${hospitalName},

I am price shopping before scheduling care. Your published transparency data lists a cash price of $${formattedPrice} for CPT ${cptCode} (${description}).

Please confirm whether that rate applies to my situation and how I can obtain a written advance estimate so my bill can align with the published amount.

Thank you.`;

    const deterministicBilled = `To the Billing Department at ${hospitalName},

I received a bill for CPT ${cptCode} (${description}). Your published transparency data lists a cash price of $${formattedPrice}.

The amount I was charged exceeds that published cash rate. Please provide a written explanation or adjust my balance if the published rate applies.

Thank you.`;

    const deterministicFallback =
        intent === 'bill_above_posted' ? deterministicBilled : deterministicShopping;

    if (!engineCache || isGenerating) {
        return deterministicFallback;
    }

    const DISPUTE_RESOLUTION_TEMPLATE =
        intent === 'price_shopping'
            ? `Write a short, polite letter to ${hospitalName}. The reader is comparing hospitals before booking. Published cash price is $${formattedPrice} for CPT ${cptCode} (${description}). They want a written estimate and confirmation of when charges could differ. Under 150 words. No placeholders. No threats.`
            : `Write a short, professional letter to ${hospitalName}. The reader already received a bill above the published cash price of $${formattedPrice} for CPT ${cptCode} (${description}). They request itemization or adjustment. Under 150 words. No placeholders. No threats.`;

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

    } catch {
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