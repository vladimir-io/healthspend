import { searchCompliance } from './compliance.js';

export function downloadDatabase(dbName: 'prices.db' | 'compliance.db') {
    const a = document.createElement('a');
    a.href = `/${dbName}`;
    a.download = `healthspend_${dbName}`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

export async function downloadScorecardCSV(stateFilter: string = '') {
    const data = await searchCompliance('', stateFilter);

    if (!data || data.length === 0) {
        alert("No data available to export.");
        return;
    }

    const headers = Object.keys(data[0]).join(',');
    const rows = data.map(record =>
        Object.values(record).map(val =>
            typeof val === 'string' ? `"${val.replace(/"/g, '""')}"` : val
        ).join(',')
    );

    const blob = new Blob([[headers, ...rows].join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = `healthspend_compliance_export_${stateFilter || 'US'}.csv`;

    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    URL.revokeObjectURL(url);
}