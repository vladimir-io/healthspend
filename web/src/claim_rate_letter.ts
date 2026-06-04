export type ClaimRateIntent = 'price_shopping' | 'bill_above_posted';

export type ClaimRateRow = {
  hospital_name?: string;
  description?: string;
  cash_price?: number;
  cpt_code?: string;
};

export function buildClaimRateDraft(
  row: ClaimRateRow,
  intent: ClaimRateIntent
): { body: string; subject: string } {
  const hospital = (row.hospital_name || 'your facility').trim();
  const desc = (row.description || 'this service').trim();
  const cpt = (row.cpt_code || '').toString().trim();
  const cptLine = cpt ? ` CPT ${cpt}` : '';
  const price = (Number(row.cash_price) || 0).toLocaleString('en-US', {
    style: 'currency',
    currency: 'USD',
  });

  if (intent === 'price_shopping') {
    const body = `Dear Billing and Patient Financial Services,

I am comparing facilities before scheduling care. ${hospital} publishes a cash rate of ${price}${cptLine ? ` for ${cptLine}` : ''} for:

${desc}

Please confirm whether this cash rate applies to my situation (self-pay, uninsured, or a written advance estimate) and what I should do before the visit so my bill can match the published amount.

If the final charge could differ from this line, please explain when it would differ and how I can obtain a binding estimate.

Thank you,
[Your name]
[Phone or email]`;

    return {
      body,
      subject: `Price estimate: published cash rate (${hospital})`,
    };
  }

  const body = `Dear Billing Department,

I received care at ${hospital} and I am reviewing my charges${cptLine ? ` for ${cptLine}` : ''}:

${desc}

Your price transparency filing lists a cash rate of ${price}. The amount I was asked to pay is above that published cash rate.

Please send a written itemization that explains any difference, or adjust my balance if the published cash rate applies to my visit.

Thank you,
[Your name]
[Account or MRN if helpful]`;

  return {
    body,
    subject: `Charges vs published cash rate (${hospital})`,
  };
}

export function billingPlaceholderEmail(hospitalName: string): string {
  const slug = hospitalName.toLowerCase().replace(/\s+/g, '');
  return `billing@${slug}.com`;
}
