document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('search-button').addEventListener('click', performSearch);
    
    document.getElementById('search-input').addEventListener('keypress', function(event) {
        if (event.key === 'Enter') {
            performSearch();
        }
    });

    // Modal close handlers
    const modal = document.getElementById('detail-modal');
    const closeBtn = modal.querySelector('.modal-close');
    
    closeBtn.addEventListener('click', closeModal);
    
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            closeModal();
        }
    });
    
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && modal.classList.contains('active')) {
            closeModal();
        }
    });
});

function openModal(doc) {
    const modal = document.getElementById('detail-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalBody = document.getElementById('modal-body');
    
    modalTitle.innerText = doc.name || 'Unknown Condition';
    
    let bodyHTML = '';
    
    // Full description
    bodyHTML += '<div class="modal-section">';
    bodyHTML += '<h4>Description</h4>';
    bodyHTML += `<p>${doc.description || 'No description available.'}</p>`;
    bodyHTML += '</div>';
    
    // Symptoms
    if (doc.symptoms) {
        bodyHTML += '<div class="modal-section">';
        bodyHTML += '<h4>Symptoms</h4>';
        bodyHTML += '<div class="badges-container">';
        let symptomList = Array.isArray(doc.symptoms) ? doc.symptoms : [doc.symptoms];
        symptomList.forEach(sym => {
            bodyHTML += `<span class="badge">${sym}</span>`;
        });
        bodyHTML += '</div></div>';
    }
    
    // Treatments
    if (doc.treatments) {
        bodyHTML += '<div class="modal-section">';
        bodyHTML += '<h4>Treatments</h4>';
        bodyHTML += '<div class="badges-container treatment-badges">';
        let treatmentList = Array.isArray(doc.treatments) ? doc.treatments : [doc.treatments];
        treatmentList.forEach(treat => {
            bodyHTML += `<span class="badge treatment-badge">${treat}</span>`;
        });
        bodyHTML += '</div></div>';
    }
    
    // Additional fields if present
    if (doc.medical_specialty) {
        bodyHTML += '<div class="modal-section">';
        bodyHTML += '<h4>Medical Specialty</h4>';
        let specialtyList = Array.isArray(doc.medical_specialty) ? doc.medical_specialty : [doc.medical_specialty];
        bodyHTML += `<p>${specialtyList.join(', ')}</p>`;
        bodyHTML += '</div>';
    }
    
    modalBody.innerHTML = bodyHTML;
    modal.classList.add('active');
    document.body.style.overflow = 'hidden';
}

function closeModal() {
    const modal = document.getElementById('detail-modal');
    modal.classList.remove('active');
    document.body.style.overflow = '';
}

async function performSearch() {
    const resultsContainer = document.getElementById('results-container');
    const keyword = document.getElementById('search-input').value.trim();
    
    // Get system
    const mode = document.getElementById('system-selector').value;

    // UI: Show searching state
    resultsContainer.innerHTML = '<h2>Searching...</h2>';

    if (keyword === '') {
        resultsContainer.innerHTML = '<p class="error-message">Please enter a search term.</p>';
        return;
    }

    const apiUrl = `http://127.0.0.1:5000/api/search?q=${encodeURIComponent(keyword)}&mode=${encodeURIComponent(mode)}`;    
    
    try {
        const response = await fetch(apiUrl);
        const data = await response.json();
        
        console.log("Results fetched using system: ", data.debug_mode_used);

        if (data.error) {
            throw new Error(data.error.msg || JSON.stringify(data.error));
        }
        
        if (!data.response || !data.response.docs) {
            throw new Error("Invalid response from Solr backend.");
        }
        
        displayResults(data.response);

    } catch (error) {
        console.error('Error fetching data:', error);
        resultsContainer.innerHTML = `<p class="error-message">Error: ${error.message}</p>`;
    }
}

function displayResults(response) {
    const resultsContainer = document.getElementById('results-container');
    resultsContainer.innerHTML = ''; 

    const docs = response.docs;
    const numFound = response.numFound;

    if (numFound === 0) {
        resultsContainer.innerHTML = `
            <div style="text-align:center; color: #666;">
                <h2>No Results Found</h2>
                <p>We couldn't find any matches. Try simpler keywords or check your spelling.</p>
            </div>`;
        return;
    }

    // Header
    resultsContainer.innerHTML = `<h3 style="color:#666; font-size: 1rem; margin-bottom: 10px;">Found ${numFound} result(s)</h3>`;

    docs.forEach(doc => {
        const item = document.createElement('div');
        item.className = 'result-item clickable';
        item.setAttribute('role', 'button');
        item.setAttribute('tabindex', '0');
        
        // Click handler to open modal
        item.addEventListener('click', () => openModal(doc));
        item.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                openModal(doc);
            }
        });

        // Title
        const title = document.createElement('h3');
        title.className = 'result-title';
        title.innerText = doc.name || 'Unknown Condition';
        item.appendChild(title);

        // Description
        const description = document.createElement('p');
        description.className = 'result-description';
        const rawDesc = doc.description || 'No description available.';
        description.innerText = rawDesc.length > 250 ? rawDesc.substring(0, 250) + '...' : rawDesc;
        item.appendChild(description);

        if (doc.symptoms) {
            const badgeContainer = document.createElement('div');
            badgeContainer.className = 'badges-container';
            
            const label = document.createElement('span');
            label.className = 'badge-label';
            label.innerText = 'Symptoms:';
            badgeContainer.appendChild(label);

            let symptomList = Array.isArray(doc.symptoms) ? doc.symptoms : [doc.symptoms];
            
            symptomList.forEach(sym => {
                const badge = document.createElement('span');
                badge.className = 'badge';
                badge.innerText = sym;
                badgeContainer.appendChild(badge);
            });
            
            item.appendChild(badgeContainer);
        }

        // Show treatment count hint
        if (doc.treatments) {
            const treatmentHint = document.createElement('p');
            treatmentHint.className = 'treatment-hint';
            let treatmentList = Array.isArray(doc.treatments) ? doc.treatments : [doc.treatments];
            treatmentHint.innerText = `💊 ${treatmentList.length} treatment${treatmentList.length !== 1 ? 's' : ''} available - Click to view`;
            item.appendChild(treatmentHint);
        }

        resultsContainer.appendChild(item);
    });
}