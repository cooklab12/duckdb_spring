// ==========================================
// MOCK DATA SERVICE (Same as dashboard)
// ==========================================
const MockDataService = {
    files: [
        {
            id: 1,
            name: 'finance_rules.json',
            modified: '2025-01-15T10:30:00Z',
            size: 24576,
            tags: ['finance', 'production', 'v2.0'],
            status: 'published',
            entryCount: 42
        },
        {
            id: 2,
            name: 'operations_dq.json',
            modified: '2025-01-14T15:45:00Z',
            size: 18432,
            tags: ['operations', 'draft'],
            status: 'draft',
            entryCount: 28
        },
        {
            id: 3,
            name: 'sales_validation.json',
            modified: '2025-01-13T09:15:00Z',
            size: 32768,
            tags: ['sales', 'validation', 'critical'],
            status: 'published',
            entryCount: 67
        },
        {
            id: 4,
            name: 'hr_compliance.json',
            modified: '2025-01-12T14:20:00Z',
            size: 12288,
            tags: ['hr', 'compliance'],
            status: 'published',
            entryCount: 15
        },
        {
            id: 5,
            name: 'it_security_rules.json',
            modified: '2025-01-10T11:00:00Z',
            size: 45056,
            tags: ['it', 'security', 'high-priority'],
            status: 'published',
            entryCount: 89
        },
        {
            id: 6,
            name: 'marketing_analytics.json',
            modified: '2025-01-08T16:30:00Z',
            size: 20480,
            tags: ['marketing', 'analytics'],
            status: 'draft',
            entryCount: 33
        },
        {
            id: 7,
            name: 'customer_data.json',
            modified: '2025-01-05T08:45:00Z',
            size: 28672,
            tags: ['customer', 'pii'],
            status: 'published',
            entryCount: 51
        }
    ],

    async getFileById(id) {
        await new Promise(resolve => setTimeout(resolve, 200));
        return this.files.find(f => f.id === id);
    },

    async getFileContent(id) {
        await new Promise(resolve => setTimeout(resolve, 300));

        // Return sample.json content
        const response = await fetch('sample.json');
        const content = await response.json();
        return content;
    }
};

// ==========================================
// GLOBAL STATE
// ==========================================
let currentFile = null;
let currentFileId = null;
let jsonData = null;
let editedEntries = new Set();
let newEntries = new Set();
let editingIndex = null;

// ==========================================
// DOM ELEMENTS
// ==========================================
// Navigation
const navDashboard = document.getElementById('navDashboard');
const navEditor = document.getElementById('navEditor');

// Editor
const loadFilesBtn = document.getElementById('addFileBtn');
const fileList = document.getElementById('fileList');
const welcomeMessage = document.getElementById('welcomeMessage');
const editorContent = document.getElementById('editorContent');
const currentFileName = document.getElementById('currentFileName');
const entryCount = document.getElementById('entryCount');
const entriesList = document.getElementById('entriesList');
const addEntryBtn = document.getElementById('addEntryBtn');
const exportBtn = document.getElementById('exportBtn');
const searchInput = document.getElementById('searchInput');
const domainFilter = document.getElementById('domainFilter');
const goToDashboardBtn = document.getElementById('goToDashboard');

// Entry Modal
const entryModal = document.getElementById('entryModal');
const entryForm = document.getElementById('entryForm');
const modalTitle = document.getElementById('modalTitle');
const cancelBtn = document.getElementById('cancelBtn');
const closeModalBtn = document.getElementById('closeModal');

// Toast
const toast = document.getElementById('toast');
const toastMessage = toast.querySelector('.toast-message');

// ==========================================
// INITIALIZATION
// ==========================================
document.addEventListener('DOMContentLoaded', async () => {
    // Initialize theme
    initTheme();

    // Navigation
    navDashboard.addEventListener('click', () => {
        window.location.href = 'dashboard.html';
    });
    navEditor.addEventListener('click', () => {
        // Already on editor
    });

    // Theme toggle
    const themeToggle = document.getElementById('themeToggle');
    themeToggle.addEventListener('click', toggleTheme);

    // Editor actions
    loadFilesBtn.addEventListener('click', loadJsonFiles);
    addEntryBtn.addEventListener('click', openAddModal);
    exportBtn.addEventListener('click', exportJson);
    searchInput.addEventListener('input', renderEntries);
    domainFilter.addEventListener('change', renderEntries);
    goToDashboardBtn.addEventListener('click', () => {
        window.location.href = 'dashboard.html';
    });

    // Entry form
    entryForm.addEventListener('submit', saveEntry);
    cancelBtn.addEventListener('click', closeModal);
    closeModalBtn.addEventListener('click', closeModal);

    // Close modal on outside click
    window.addEventListener('click', (e) => {
        if (e.target === entryModal) closeModal();
    });

    // Tab key support for DSL
    const dslTextarea = document.getElementById('dsl');
    if (dslTextarea) {
        dslTextarea.addEventListener('keydown', function(e) {
            if (e.key === 'Tab') {
                e.preventDefault();
                const start = this.selectionStart;
                const end = this.selectionEnd;
                this.value = this.value.substring(0, start) + '    ' + this.value.substring(end);
                this.selectionStart = this.selectionEnd = start + 4;
            }
        });
    }

    // Check if file was selected from dashboard
    const selectedFileId = localStorage.getItem('selectedFileId');
    if (selectedFileId) {
        localStorage.removeItem('selectedFileId');
        await openFileForEdit(parseInt(selectedFileId));
    }
});

// ==========================================
// THEME MANAGEMENT
// ==========================================
function initTheme() {
    const savedTheme = localStorage.getItem('theme');
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    if (savedTheme) {
        document.documentElement.setAttribute('data-theme', savedTheme);
    } else if (prefersDark) {
        document.documentElement.setAttribute('data-theme', 'dark');
    } else {
        document.documentElement.setAttribute('data-theme', 'light');
    }
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
}

// ==========================================
// FILE LOADING
// ==========================================
async function openFileForEdit(fileId) {
    try {
        const file = await MockDataService.getFileById(fileId);
        if (!file) {
            showToast('File not found', 'error');
            return;
        }

        currentFileId = fileId;
        currentFile = file.name;

        // Load actual content
        jsonData = await MockDataService.getFileContent(fileId);

        // Reset state
        editedEntries.clear();
        newEntries.clear();

        // Update UI
        currentFileName.textContent = file.name;
        welcomeMessage.style.display = 'none';
        editorContent.classList.add('active');

        // Update domain filter
        updateDomainFilter();

        // Render entries
        renderEntries();

        // Add to sidebar
        addFileToSidebar(file);

        showToast(`Loaded ${file.name}`, 'success');
    } catch (error) {
        showToast('Failed to load file', 'error');
        console.error(error);
    }
}

function addFileToSidebar(file) {
    const existingItem = fileList.querySelector(`[data-file-id="${file.id}"]`);
    if (existingItem) {
        document.querySelectorAll('.file-item').forEach(item => item.classList.remove('active'));
        existingItem.classList.add('active');
        return;
    }

    const fileItem = document.createElement('div');
    fileItem.className = 'file-item active';
    fileItem.dataset.fileId = file.id;
    fileItem.innerHTML = `
        <span class="file-icon">📄</span>
        <span class="file-name">${escapeHtml(file.name)}</span>
    `;

    fileItem.addEventListener('click', () => openFileForEdit(file.id));

    document.querySelectorAll('.file-item').forEach(item => item.classList.remove('active'));
    fileList.appendChild(fileItem);
}

async function loadJsonFiles() {
    const input = document.createElement('input');
    input.type = 'file';
    input.multiple = true;
    input.accept = '.json';

    input.onchange = async (e) => {
        const files = Array.from(e.target.files);

        for (const file of files) {
            const fileItem = document.createElement('div');
            fileItem.className = 'file-item';
            fileItem.innerHTML = `
                <span class="file-icon">📄</span>
                <span class="file-name">${escapeHtml(file.name)}</span>
            `;

            fileItem.addEventListener('click', (e) => loadFileContent(file, e.currentTarget));

            fileList.appendChild(fileItem);
        }

        showToast(`Loaded ${files.length} file(s)`, 'success');
    };

    input.click();
}

async function loadFileContent(file, fileItemElement) {
    try {
        const text = await file.text();

        try {
            jsonData = JSON.parse(text);
        } catch (parseError) {
            console.error('JSON Parse Error:', parseError);
            showToast(`JSON Parse Error: ${parseError.message}`, 'error');
            return;
        }

        if (!jsonData || !jsonData.data || !jsonData.data.activeRuleWithoutLinkage) {
            showToast('Invalid JSON structure. Expected: data.activeRuleWithoutLinkage array', 'error');
            return;
        }

        document.querySelectorAll('.file-item').forEach(item => {
            item.classList.remove('active');
        });
        fileItemElement.classList.add('active');

        currentFile = file.name;
        currentFileName.textContent = file.name;

        editedEntries.clear();
        newEntries.clear();

        welcomeMessage.style.display = 'none';
        editorContent.classList.add('active');

        updateDomainFilter();
        renderEntries();

        showToast(`Loaded ${file.name} with ${jsonData.data.activeRuleWithoutLinkage.length} entries`, 'success');
    } catch (error) {
        showToast(`Error loading file: ${error.message}`, 'error');
        console.error('Load error:', error);
    }
}

// ==========================================
// DOMAIN FILTER & ENTRIES
// ==========================================
function updateDomainFilter() {
    if (!jsonData || !jsonData.data || !jsonData.data.activeRuleWithoutLinkage) {
        return;
    }

    const domains = new Set();
    jsonData.data.activeRuleWithoutLinkage.forEach(entry => {
        if (entry.domain) {
            domains.add(entry.domain);
        }
    });

    const currentFilter = domainFilter.value;
    domainFilter.innerHTML = '<option value="">All Domains</option>';

    Array.from(domains).sort().forEach(domain => {
        const option = document.createElement('option');
        option.value = domain;
        option.textContent = domain;
        domainFilter.appendChild(option);
    });

    domainFilter.value = currentFilter;
}

function renderEntries() {
    if (!jsonData || !jsonData.data || !jsonData.data.activeRuleWithoutLinkage) {
        entriesList.innerHTML = '<p style="text-align: center; color: var(--text-tertiary); padding: 40px;">No entries found</p>';
        return;
    }

    entryCount.textContent = `${jsonData.data.activeRuleWithoutLinkage.length} entries`;

    const searchTerm = searchInput.value.toLowerCase();
    const filterDomain = domainFilter.value;

    const filteredEntries = jsonData.data.activeRuleWithoutLinkage
        .map((entry, index) => ({ ...entry, originalIndex: index }))
        .filter(entry => {
            const matchesSearch = !searchTerm ||
                entry.name?.toLowerCase().includes(searchTerm) ||
                entry.key?.toLowerCase().includes(searchTerm) ||
                entry.description?.toLowerCase().includes(searchTerm) ||
                entry.domain?.toLowerCase().includes(searchTerm) ||
                entry.errorCode?.toLowerCase().includes(searchTerm);

            const matchesDomain = !filterDomain || entry.domain === filterDomain;

            return matchesSearch && matchesDomain;
        });

    if (filteredEntries.length === 0) {
        entriesList.innerHTML = '<p style="text-align: center; color: var(--text-tertiary); padding: 40px;">No matching entries found</p>';
        return;
    }

    entriesList.innerHTML = filteredEntries.map(entry => createEntryCard(entry)).join('');
}

function createEntryCard(entry) {
    const isEdited = editedEntries.has(entry.originalIndex);
    const isNew = newEntries.has(entry.originalIndex);

    const cardClass = ['entry-card'];
    if (isNew) cardClass.push('new');
    else if (isEdited) cardClass.push('edited');

    const activeBadge = entry.isActive === 'true' || entry.isActive === true
        ? '<span class="badge badge-active">Active</span>'
        : '<span class="badge badge-inactive">Inactive</span>';

    const severityClass = `badge-${(entry.severity || 'info').toLowerCase()}`;

    const filterRf = Array.isArray(entry.filterRf) ? entry.filterRf.join(', ') : entry.filterRf || 'N/A';
    const dsl = Array.isArray(entry.dsl) ? entry.dsl.join('\n') : entry.dsl || 'N/A';

    return `
        <div class="${cardClass.join(' ')}" data-index="${entry.originalIndex}">
            <div class="entry-header">
                <div class="entry-title">
                    <span class="entry-name">${escapeHtml(entry.name || 'Unnamed')}</span>
                    <div class="entry-badges">
                        ${activeBadge}
                        <span class="badge ${severityClass}">${escapeHtml(entry.severity || 'INFO')}</span>
                        <span class="badge badge-domain">${escapeHtml(entry.domain || 'N/A')}</span>
                        ${isNew ? '<span class="badge badge-info">NEW</span>' : ''}
                        ${isEdited && !isNew ? '<span class="badge badge-warning">EDITED</span>' : ''}
                    </div>
                </div>
                <div class="entry-actions">
                    <button class="btn btn-edit btn-icon" onclick="editEntry(${entry.originalIndex})">Edit</button>
                    <button class="btn btn-danger btn-icon" onclick="deleteEntry(${entry.originalIndex})">Delete</button>
                </div>
            </div>
            <div class="entry-details">
                <div class="detail-item">
                    <span class="detail-label">Key</span>
                    <span class="detail-value code">${escapeHtml(entry.key || 'N/A')}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Application ID</span>
                    <span class="detail-value code">${escapeHtml(entry.applicationId || 'N/A')}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Error Code</span>
                    <span class="detail-value code">${escapeHtml(entry.errorCode || 'N/A')}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Type Code</span>
                    <span class="detail-value code">${escapeHtml(entry.typeCd || 'N/A')}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Start Date</span>
                    <span class="detail-value">${entry.startDt || 'N/A'}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">End Date</span>
                    <span class="detail-value">${entry.endDt || 'Active'}</span>
                </div>
                <div class="detail-item" style="grid-column: 1 / -1;">
                    <span class="detail-label">Description</span>
                    <span class="detail-value">${escapeHtml(entry.description || 'N/A')}</span>
                </div>
                <div class="detail-item">
                    <span class="detail-label">Filter References</span>
                    <span class="detail-value code">${escapeHtml(filterRf)}</span>
                </div>
                <div class="detail-item" style="grid-column: 1 / -1;">
                    <span class="detail-label">DSL Query</span>
                    <span class="detail-value code-large">${escapeHtml(dsl)}</span>
                </div>
            </div>
        </div>
    `;
}

// ==========================================
// ENTRY MODAL
// ==========================================
function openAddModal() {
    editingIndex = null;
    modalTitle.textContent = 'Add New Entry';
    entryForm.reset();
    entryModal.classList.add('active');
}

function editEntry(index) {
    editingIndex = index;
    const entry = jsonData.data.activeRuleWithoutLinkage[index];

    modalTitle.textContent = 'Edit Entry';

    document.getElementById('key').value = entry.key || '';
    document.getElementById('isActive').value = entry.isActive || 'true';
    document.getElementById('applicationId').value = entry.applicationId || '';
    document.getElementById('dqCd').value = entry.dqCd || '';
    document.getElementById('name').value = entry.name || '';
    document.getElementById('description').value = entry.description || '';
    document.getElementById('domain').value = entry.domain || '';
    document.getElementById('errorCode').value = entry.errorCode || '';
    document.getElementById('typeCd').value = entry.typeCd || '';
    document.getElementById('severity').value = entry.severity || 'ERROR';
    document.getElementById('startDt').value = entry.startDt || '';
    document.getElementById('endDt').value = entry.endDt || '';
    document.getElementById('filterRf').value = Array.isArray(entry.filterRf) ? entry.filterRf.join(', ') : (entry.filterRf || '');
    document.getElementById('dsl').value = Array.isArray(entry.dsl) ? entry.dsl.join('\n') : (entry.dsl || '');

    entryModal.classList.add('active');
}

function saveEntry(e) {
    e.preventDefault();

    const formData = {
        key: document.getElementById('key').value,
        isActive: document.getElementById('isActive').value,
        applicationId: document.getElementById('applicationId').value,
        dqCd: document.getElementById('dqCd').value,
        name: document.getElementById('name').value,
        description: document.getElementById('description').value,
        domain: document.getElementById('domain').value,
        errorCode: document.getElementById('errorCode').value,
        typeCd: document.getElementById('typeCd').value,
        severity: document.getElementById('severity').value,
        startDt: document.getElementById('startDt').value,
        endDt: document.getElementById('endDt').value,
        filterRf: document.getElementById('filterRf').value.split(',').map(s => s.trim()).filter(s => s),
        dsl: document.getElementById('dsl').value.split('\n').map(s => s.trim()).filter(s => s)
    };

    if (editingIndex !== null) {
        jsonData.data.activeRuleWithoutLinkage[editingIndex] = formData;
        editedEntries.add(editingIndex);
        showToast('Entry updated successfully', 'success');
    } else {
        jsonData.data.activeRuleWithoutLinkage.push(formData);
        newEntries.add(jsonData.data.activeRuleWithoutLinkage.length - 1);
        showToast('New entry added successfully', 'success');
    }

    closeModal();
    updateDomainFilter();
    renderEntries();
}

function deleteEntry(index) {
    if (confirm('Are you sure you want to delete this entry?')) {
        jsonData.data.activeRuleWithoutLinkage.splice(index, 1);

        const newEdited = new Set();
        editedEntries.forEach(i => {
            if (i < index) newEdited.add(i);
            else if (i > index) newEdited.add(i - 1);
        });
        editedEntries = newEdited;

        const newNewEntries = new Set();
        newEntries.forEach(i => {
            if (i < index) newNewEntries.add(i);
            else if (i > index) newNewEntries.add(i - 1);
        });
        newEntries = newNewEntries;

        showToast('Entry deleted successfully', 'success');
        updateDomainFilter();
        renderEntries();
    }
}

function closeModal() {
    entryModal.classList.remove('active');
    entryForm.reset();
    editingIndex = null;
}

// ==========================================
// EXPORT
// ==========================================
function exportJson() {
    if (!jsonData) {
        showToast('No data to export', 'error');
        return;
    }

    const jsonStr = JSON.stringify(jsonData, null, '\t');
    const blob = new Blob([jsonStr], { type: 'application/json' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = currentFile ? `updated_${currentFile}` : 'updated_data.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    showToast('JSON exported successfully', 'success');
}

// ==========================================
// TOAST
// ==========================================
function showToast(message, type = 'success') {
    toastMessage.textContent = message;
    toast.className = `toast ${type} show`;

    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}

// ==========================================
// UTILITIES
// ==========================================
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
