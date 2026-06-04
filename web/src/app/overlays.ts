import {
  billingPlaceholderEmail,
  buildClaimRateDraft,
  type ClaimRateIntent,
} from '../claim_rate_letter.js';
import { closeAllSheets, closeSheet, openSheet } from './micro.js';

let lastDisputeRow: Record<string, unknown> | null = null;

function getDisputeIntent(): ClaimRateIntent {
  const el = document.querySelector('input[name="dispute-intent"]:checked') as HTMLInputElement | null;
  return el?.value === 'bill_above_posted' ? 'bill_above_posted' : 'price_shopping';
}

function generateGmailLink({ to, subject, body }: { to: string; subject: string; body: string }) {
  return `https://mail.google.com/mail/?view=cm&fs=1&to=${to}&su=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

function generateOutlookLink({ to, subject, body }: { to: string; subject: string; body: string }) {
  return `https://outlook.office.com/mail/deeplink/compose?to=${to}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

function copyToClipboard(text: string) {
  void navigator.clipboard.writeText(text);
}

function disputeRecipientEmail(): string {
  const custom = (document.getElementById('dispute-to-email') as HTMLInputElement | null)?.value?.trim();
  if (custom) return custom;
  return billingPlaceholderEmail(String(lastDisputeRow?.hospital_name || ''));
}

function syncDisputeMailLinks() {
  if (!lastDisputeRow) return;
  const draft = document.getElementById('dispute-draft') as HTMLTextAreaElement | null;
  const btnDGmail = document.getElementById('btn-dispute-gmail') as HTMLAnchorElement | null;
  const btnDOutlook = document.getElementById('btn-dispute-outlook') as HTMLAnchorElement | null;
  const body = draft?.value ?? '';
  const { subject } = buildClaimRateDraft(lastDisputeRow as Parameters<typeof buildClaimRateDraft>[0], getDisputeIntent());
  const to = disputeRecipientEmail();
  if (btnDGmail) btnDGmail.href = generateGmailLink({ to, subject, body });
  if (btnDOutlook) btnDOutlook.href = generateOutlookLink({ to, subject, body });
}

function applyDisputeDraft(row: Record<string, unknown>) {
  const disputeDraft = document.getElementById('dispute-draft') as HTMLTextAreaElement | null;
  const { body } = buildClaimRateDraft(row as Parameters<typeof buildClaimRateDraft>[0], getDisputeIntent());
  if (disputeDraft) disputeDraft.value = body;
  syncDisputeMailLinks();
}

export function handleDispute(row: Record<string, unknown>) {
  lastDisputeRow = row;
  const disputeOverlay = document.getElementById('dispute-overlay') as HTMLDivElement;
  const disputeDraft = document.getElementById('dispute-draft') as HTMLTextAreaElement;
  const btnDClip = document.getElementById('btn-dispute-copy') as HTMLButtonElement;
  const shopping = document.getElementById('dispute-intent-shopping') as HTMLInputElement | null;
  const toField = document.getElementById('dispute-to-email') as HTMLInputElement | null;
  if (toField) {
    toField.value = '';
    toField.placeholder = `e.g. billing@${billingPlaceholderEmail(String(row.hospital_name || '')).split('@')[1] || 'hospital.org'}`;
  }
  if (shopping) shopping.checked = true;
  applyDisputeDraft(row);

  if (btnDClip) {
    btnDClip.onclick = () => {
      copyToClipboard(disputeDraft?.value ?? '');
      const oldTxt = btnDClip.innerText;
      btnDClip.innerText = 'COPIED ✓';
      setTimeout(() => {
        btnDClip.innerText = oldTxt;
      }, 2000);
    };
  }
  if (disputeOverlay) openSheet(disputeOverlay);
}

export function handleDraft(row: Record<string, unknown>, observedReason: string) {
  const overlay = document.getElementById('letter-overlay') as HTMLDivElement;
  const letterDraft = document.getElementById('letter-draft') as HTMLTextAreaElement;
  const btnGmail = document.getElementById('btn-gmail') as HTMLAnchorElement;
  const btnOutlook = document.getElementById('btn-outlook') as HTMLAnchorElement;
  const subjectLine = `Formal Complaint of Noncompliance: ${row.hospital_name}`;
  const toEmail = 'HPTCompliance@cms.hhs.gov';
  const letter = `To the CMS Price Transparency Enforcement Division:

I am submitting a formal complaint regarding suspected noncompliance by ${row.hospital_name} with the Hospital Price Transparency Rule (45 CFR § 180.50).

Observed issue:
${observedReason}

An independent review of this facility's data catalog revealed issues regarding CPT Code ${row.cpt_code}. Under current CMS rules, hospitals must publish actual dollar amounts in machine-readable files.

I respectfully request that CMS review this facility's disclosures.

Sincerely,
[Your Full Name]
[Your Contact Information / Zip Code]`;

  if (letterDraft) letterDraft.value = letter;
  if (btnGmail) btnGmail.href = generateGmailLink({ to: toEmail, subject: subjectLine, body: letter });
  if (btnOutlook) btnOutlook.href = generateOutlookLink({ to: toEmail, subject: subjectLine, body: letter });
  if (overlay) openSheet(overlay);
}

export function setupOverlays(): void {
  const overlay = document.getElementById('letter-overlay') as HTMLDivElement;
  const btnCloseLetter = document.getElementById('btn-close-letter') as HTMLButtonElement;
  const btnCopy = document.getElementById('btn-copy') as HTMLButtonElement;
  const letterDraft = document.getElementById('letter-draft') as HTMLTextAreaElement;

  document.querySelectorAll('.sheet-backdrop').forEach((bd) => {
    bd.addEventListener('click', () => closeAllSheets());
  });
  btnCloseLetter?.addEventListener('click', () => {
    if (overlay) closeSheet(overlay);
  });
  document.getElementById('btn-close-dispute')?.addEventListener('click', () => {
    const d = document.getElementById('dispute-overlay');
    if (d) closeSheet(d);
  });

  document.getElementById('dispute-overlay')?.addEventListener('change', (ev) => {
    const t = ev.target as HTMLInputElement | null;
    if (!t || t.name !== 'dispute-intent' || !lastDisputeRow) return;
    applyDisputeDraft(lastDisputeRow);
  });

  document.getElementById('dispute-draft')?.addEventListener('input', () => syncDisputeMailLinks());
  document.getElementById('dispute-to-email')?.addEventListener('input', () => syncDisputeMailLinks());

  btnCopy?.addEventListener('click', () => {
    copyToClipboard(letterDraft?.value ?? '');
    const oldText = btnCopy.innerText;
    btnCopy.innerText = 'COPIED ✓';
    setTimeout(() => {
      btnCopy.innerText = oldText;
    }, 2000);
  });
}
