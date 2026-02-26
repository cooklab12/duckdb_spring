// ==========================================
// MOCK DATA SERVICE
// ==========================================
const MockDataService = {
    files: [
        {
            id: 1,
            name: 'finance_rules.json',
            modified: '2025-01-15T10:30:00Z',
            size: 24576,
            tags: ['finance', 'production', 'v2.0'],
            region: 'NAM',
            entryCount: 42
        },
        {
            id: 2,
            name: 'operations_dq.json',
            modified: '2025-01-14T15:45:00Z',
            size: 18432,
            tags: ['operations', 'draft'],
            region: 'EMEA',
            entryCount: 28
        },
        {
            id: 3,
            name: 'sales_validation.json',
            modified: '2025-01-13T09:15:00Z',
            size: 32768,
            tags: ['sales', 'validation', 'critical'],
            region: 'APAC',
            entryCount: 67
        },
        {
            id: 4,
            name: 'hr_compliance.json',
            modified: '2025-01-12T14:20:00Z',
            size: 12288,
            tags: ['hr', 'compliance'],
            region: 'NAM',
            entryCount: 15
        },
        {
            id: 5,
            name: 'it_security_rules.json',
            modified: '2025-01-10T11:00:00Z',
            size: 45056,
            tags: ['it', 'security', 'high-priority'],
            region: 'EMEA',
            entryCount: 89
        },
        {
            id: 6,
            name: 'marketing_analytics.json',
            modified: '2025-01-08T16:30:00Z',
            size: 20480,
            tags: ['marketing', 'analytics'],
            region: 'APAC',
            entryCount: 33
        },
        {
            id: 7,
            name: 'customer_data.json',
            modified: '2025-01-05T08:45:00Z',
            size: 28672,
            tags: ['customer', 'pii'],
            region: 'NAM',
            entryCount: 51
        }
    ],

    async getFiles() {
        // Simulate API delay
        await new Promise(resolve => setTimeout(resolve, 200));
        return [...this.files];
    },

    async getFileById(id) {
        await new Promise(resolve => setTimeout(resolve, 200));
        return this.files.find(f => f.id === id);
    },

    async updateFileTags(id, tags) {
        await new Promise(resolve => setTimeout(resolve, 200));
        const file = this.files.find(f => f.id === id);
        if (file) {
            file.tags = tags;
            return file;
        }
        throw new Error('File not found');
    },

    async deleteFile(id) {
        await new Promise(resolve => setTimeout(resolve, 200));
        const index = this.files.findIndex(f => f.id === id);
        if (index !== -1) {
            this.files.splice(index, 1);
            return true;
        }
        throw new Error('File not found');
    },

    // Get actual JSON content for a file
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
let filesData = [];
let currentEditingTagsFileId = null;
let tempTags = [];

// ==========================================
// DOM ELEMENTS
// ==========================================
// Navigation
const navDashboard = document.getElementById('navDashboard');
const navEditor = document.getElementById('navEditor');

// Dashboard
const fileSearch = document.getElementById('fileSearch');
const sortSelect = document.getElementById('sortSelect');
const filesTable = document.getElementById('filesTable');
const regionInputs = document.querySelectorAll('input[name="region"]');

// Tag modal
const tagModal = document.getElementById('tagModal');
const closeTagModalBtn = document.getElementById('closeTagModal');
const cancelTagBtn = document.getElementById('cancelTagBtn');
const saveTagBtn = document.getElementById('saveTagBtn');
const tagInput = document.getElementById('tagInput');
const tagListEdit = document.getElementById('tagList');

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
        // Already on dashboard
    });
    navEditor.addEventListener('click', () => {
        window.location.href = 'editor.html';
    });

    // Theme toggle
    const themeToggle = document.getElementById('themeToggle');
    themeToggle.addEventListener('click', toggleTheme);

    // Dashboard actions
    fileSearch.addEventListener('input', renderFilesTable);
    sortSelect.addEventListener('change', renderFilesTable);

    // Region filter
    regionInputs.forEach(input => {
        input.addEventListener('change', renderFilesTable);
    });

    // Tag modal
    closeTagModalBtn.addEventListener('click', closeTagModal);
    cancelTagBtn.addEventListener('click', closeTagModal);
    saveTagBtn.addEventListener('click', saveTags);
    tagInput.addEventListener('keydown', handleTagInput);

    // Close modals on outside click
    window.addEventListener('click', (e) => {
        if (e.target === tagModal) closeTagModal();
    });

    // Load initial data
    await loadDashboardData();
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
// DASHBOARD FUNCTIONS
// ==========================================
async function loadDashboardData() {
    try {
        filesData = await MockDataService.getFiles();
        renderFilesTable();
    } catch (error) {
        showToast('Failed to load files', 'error');
        console.error(error);
    }
}

function renderFilesTable() {
    const searchTerm = fileSearch.value.toLowerCase();
    const sortBy = sortSelect.value;
    const selectedRegion = document.querySelector('input[name="region"]:checked')?.value || 'NAM';

    let filteredFiles = filesData.filter(file => {
        const matchesSearch = file.name.toLowerCase().includes(searchTerm) ||
               file.tags.some(tag => tag.toLowerCase().includes(searchTerm));
        const matchesRegion = file.region === selectedRegion;
        return matchesSearch && matchesRegion;
    });

    // Sort
    filteredFiles.sort((a, b) => {
        if (sortBy === 'name') return a.name.localeCompare(b.name);
        if (sortBy === 'size') return b.size - a.size;
        return new Date(b.modified) - new Date(a.modified);
    });

    if (filteredFiles.length === 0) {
        filesTable.innerHTML = `
            <div class="empty-state">
                <p>No files found</p>
            </div>
        `;
        return;
    }

    const headerHtml = `
        <div class="files-table-header">
            <div class="header-cell"></div>
            <div class="header-cell">Name</div>
            <div class="header-cell">Modified</div>
            <div class="header-cell">Tags</div>
            <div class="header-cell">Size</div>
            <div class="header-cell"></div>
        </div>
    `;

    const rowsHtml = filteredFiles.map(file => {
        const date = new Date(file.modified);
        const formattedDate = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        const formattedTime = date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
        const formattedSize = formatFileSize(file.size);
        const tagsHtml = file.tags.slice(0, 2).map(tag =>
            `<span class="file-tag">${escapeHtml(tag)}</span>`
        ).join('');
        const moreTags = file.tags.length > 2 ? `<span class="file-tag">+${file.tags.length - 2}</span>` : '';

        return `
            <div class="file-row" data-file-id="${file.id}">
                <div class="file-checkbox" onclick="event.stopPropagation()">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                        <polyline points="20 6 9 17 4 12"/>
                    </svg>
                </div>
                <div class="file-name-cell">
                    <span class="file-icon">📄</span>
                    <div>
                        <div class="file-name-text">${escapeHtml(file.name)}</div>
                        <div class="file-meta">${file.entryCount} entries</div>
                    </div>
                </div>
                <div class="file-meta-cell">
                    <div class="file-meta">${formattedDate}</div>
                    <div class="file-meta">${formattedTime}</div>
                </div>
                <div class="file-tags-cell">
                    <div class="file-tags">${tagsHtml}${moreTags}</div>
                </div>
                <div class="file-size-cell">
                    <div class="file-size">${formattedSize}</div>
                </div>
                <div class="file-actions-cell">
                    <button class="icon-btn" onclick="openTagModal(${file.id})" title="Edit tags">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/>
                            <line x1="7" y1="7" x2="7.01" y2="7"/>
                        </svg>
                    </button>
                    <button class="icon-btn danger" onclick="deleteFile(${file.id})" title="Delete file">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="3 6 5 6 21 6"/>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                        </svg>
                    </button>
                </div>
            </div>
        `;
    }).join('');

    filesTable.innerHTML = headerHtml + rowsHtml;

    // Add click handlers for file rows
    document.querySelectorAll('.file-row').forEach(row => {
        row.addEventListener('click', () => {
            const fileId = row.dataset.fileId;
            // Store file ID and navigate to editor
            localStorage.setItem('selectedFileId', fileId);
            window.location.href = 'editor.html';
        });
    });
}

function formatFileSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

// ==========================================
// TAG MANAGEMENT
// ==========================================
function openTagModal(fileId) {
    event.stopPropagation();
    currentEditingTagsFileId = fileId;
    const file = filesData.find(f => f.id === fileId);
    if (!file) return;

    tempTags = [...file.tags];
    renderTagList();
    tagModal.classList.add('active');
    tagInput.focus();
}

function closeTagModal() {
    tagModal.classList.remove('active');
    currentEditingTagsFileId = null;
    tempTags = [];
}

function renderTagList() {
    tagListEdit.innerHTML = tempTags.map(tag =>
        `<span class="tag-item">
            ${escapeHtml(tag)}
            <button type="button" class="tag-item-remove" onclick="removeTag('${escapeHtml(tag)}')">
                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3">
                    <line x1="18" y1="6" x2="6" y2="18"/>
                    <line x1="6" y1="6" x2="18" y2="18"/>
                </svg>
            </button>
        </span>`
    ).join('');
}

function handleTagInput(e) {
    if (e.key === 'Enter' || e.key === ',') {
        e.preventDefault();
        const value = tagInput.value.trim().replace(',', '');
        if (value && !tempTags.includes(value)) {
            tempTags.push(value);
            renderTagList();
            tagInput.value = '';
        }
    }
}

function removeTag(tag) {
    tempTags = tempTags.filter(t => t !== tag);
    renderTagList();
}

async function saveTags() {
    if (!currentEditingTagsFileId) return;

    try {
        await MockDataService.updateFileTags(currentEditingTagsFileId, tempTags);
        const file = filesData.find(f => f.id === currentEditingTagsFileId);
        if (file) {
            file.tags = tempTags;
        }

        renderFilesTable();
        closeTagModal();
        showToast('Tags updated', 'success');
    } catch (error) {
        showToast('Failed to update tags', 'error');
        console.error(error);
    }
}

async function deleteFile(fileId) {
    event.stopPropagation();

    if (!confirm('Are you sure you want to delete this file?')) return;

    try {
        await MockDataService.deleteFile(fileId);
        filesData = filesData.filter(f => f.id !== fileId);

        renderFilesTable();
        showToast('File deleted', 'success');
    } catch (error) {
        showToast('Failed to delete file', 'error');
        console.error(error);
    }
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
