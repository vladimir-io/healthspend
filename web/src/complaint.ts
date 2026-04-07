export interface ComplaintContext {
    name: string;
    ccn: string;
    state: string;
    city: string;
    score: number;
    txt_exists: boolean | number;
    robots_ok: boolean | number;
    mrf_machine_readable?: boolean | number;
    waf_blocked?: boolean | number;
    evidence_json?: string;
}

export function generateComplaintLetter(ctx: ComplaintContext): string {
    const violations: string[] = [];

    if (!ctx.txt_exists) {
        violations.push(
            'Failure to publish a machine-readable file (MRF) discoverable via the required "cms-hpt.txt" index file, in violation of 45 CFR §180.50(d)(2).'
        );
    }

    if (!ctx.robots_ok) {
        violations.push(
            'Active use of a "robots.txt" directive to instruct web crawlers to disallow access to the hospital\'s MRF, a deliberate obstruction of public access in violation of 45 CFR §180.60.'
        );
    }

    if (ctx.mrf_machine_readable === false || ctx.mrf_machine_readable === 0) {
        violations.push(
            'Publication of an MRF file that is not machine-readable, containing malformed data, non-standard schemas, or non-parseable formatting, in violation of 45 CFR §180.50(b).'
        );
    }

    if (ctx.waf_blocked === true || ctx.waf_blocked === 1) {
        violations.push(
            'Deployment of a Web Application Firewall (WAF) or bot-mitigation technology that blocks automated access to legally mandated public pricing data, in violation of 45 CFR §180.60.'
        );
    }

    if (violations.length === 0) {
        violations.push(
            'Failure to meet the minimum standards required under the Hospital Price Transparency Rule (45 CFR Part 180) as determined by an independent compliance audit.'
        );
    }

    const numberedViolations = violations.map((v, i) => `  ${i + 1}. ${v}`).join('\n\n');
    const today = new Date().toLocaleDateString('en-US', {
        year: 'numeric', month: 'long', day: 'numeric'
    });

    return `${today}

Centers for Medicare & Medicaid Services
Price Transparency Enforcement
HPTCompliance@cms.hhs.gov

Re: Formal Complaint: Hospital Price Transparency Non-Compliance: ${ctx.name} (CCN: ${ctx.ccn})

To the CMS Price Transparency Enforcement Division,

I am formally submitting this complaint regarding the failure of ${ctx.name}, located in ${ctx.city}, ${ctx.state} (CMS Certification Number: ${ctx.ccn}), to comply with the Hospital Price Transparency Rule enacted under the Affordable Care Act.

An independent audit conducted via publicly available HTTP inspection has identified ${violations.length} specific violation${violations.length > 1 ? 's' : ''} of federal regulations:

${numberedViolations}

This hospital has received a compliance score of ${ctx.score}/100 in this audit. Under 42 CFR §180.70, CMS has authority to impose Civil Monetary Penalties (CMPs) of up to $300 per day for hospitals with fewer than 30 beds, and up to $5,500 per day for larger institutions. I request that CMS:

  1. Open a formal investigation into the above violations.
  2. Issue a corrective action plan (CAP) to ${ctx.name} within 30 days.
  3. Impose Civil Monetary Penalties if compliance is not achieved within the CAP window.
  4. Publish the outcome of this investigation on the CMS website for public accountability.

The right to access hospital pricing information is a federal mandate. Obstruction of that right harms patients' ability to make informed healthcare decisions. I respectfully request CMS enforce this law with the full authority provided to it.

Sincerely,

[Your Name]`;
}