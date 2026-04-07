import { generateComplaintLetter, type ComplaintContext } from './complaint.js';

export interface LetterParams {
    to: string;
    subject: string;
    body: string;
}

export function generateGmailLink({ to, subject, body }: LetterParams): string {
    return `https://mail.google.com/mail/?view=cm&fs=1&to=${encodeURIComponent(to)}&su=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

export function generateOutlookLink({ to, subject, body }: LetterParams): string {
    return `https://outlook.live.com/mail/0/deeplink/compose?to=${encodeURIComponent(to)}&subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

export function generateYahooLink({ to, subject, body }: LetterParams): string {
    return `https://compose.mail.yahoo.com/?to=${encodeURIComponent(to)}&subj=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
}

export function copyToClipboard(text: string): boolean {
    if (navigator.clipboard && window.isSecureContext) {
        navigator.clipboard.writeText(text);
        return true;
    }
    return false;
}

export function handleComplaint(record: any) {
    const overlay = document.getElementById('letter-overlay') as HTMLDivElement;
    if (!overlay) return;

    const ctx: ComplaintContext = {
        name: record.name,
        ccn: record.ccn,
        state: record.state,
        city: record.city,
        score: record.score,
        txt_exists: record.txt_exists,
        robots_ok: record.robots_ok
    };

    const draft = generateComplaintLetter(ctx);
    const to = 'HPTCompliance@cms.hhs.gov';
    const subject = `Official Complaint: Hospital Noncompliance : CCN ${record.ccn}`;

    const textarea = document.getElementById('letter-draft') as HTMLTextAreaElement;
    const btnGmail = document.getElementById('btn-gmail') as HTMLAnchorElement;
    const btnCopy = document.getElementById('btn-copy') as HTMLButtonElement;
    const btnClose = document.getElementById('btn-close-letter') as HTMLButtonElement;
    const backdrop = overlay.querySelector('.sheet-backdrop') as HTMLDivElement;

    if (textarea) textarea.value = draft;

    if (btnGmail) {
        btnGmail.href = generateGmailLink({ to, subject, body: draft });
    }

    if (btnCopy) {
        btnCopy.onclick = () => {
            copyToClipboard(draft);
            const orig = btnCopy.innerText;
            btnCopy.innerText = 'Copied ✓';
            setTimeout(() => { btnCopy.innerText = orig; }, 2000);
        };
    }

    const closeOverlay = () => overlay.classList.add('hidden');
    btnClose?.addEventListener('click', closeOverlay, { once: true });
    backdrop?.addEventListener('click', closeOverlay, { once: true });

    overlay.classList.remove('hidden');
}
