// Helper function to clean text
function cleanText(text) {
    if (!text) 
        return 'Unknown';
    let clean = text.replace(/_/g, ' ');
    return clean.replace(/\b\w/g, char => char.toUpperCase());
}

// Helper function to push state to browser history without reloading
function updateURL() {
    const query = document.getElementById('search-input').value;
    const mode = document.getElementById('system-selector').value;
    
    if (query) {
        const newUrl = `${window.location.pathname}?q=${encodeURIComponent(query)}&mode=${encodeURIComponent(mode)}`;
        window.history.pushState({ path: newUrl }, '', newUrl);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const searchInput = document.getElementById('search-input');
    const systemSelector = document.getElementById('system-selector');

    const params = new URLSearchParams(window.location.search);
    const urlQuery = params.get('q');
    const urlMode = params.get('mode');

    if (urlQuery) {
        searchInput.value = urlQuery;
        if (urlMode) {
            systemSelector.value = urlMode;
        }
        performSearch();
    }

    document.getElementById('search-button').addEventListener('click', () => {
        updateURL();
        performSearch();
    });
    
    searchInput.addEventListener('keypress', function(event) {
        if (event.key === 'Enter') {
            updateURL();
            performSearch();
        }
    });
    
    systemSelector.addEventListener('change', () => {
        if (searchInput.value.trim() !== "") {
            updateURL();
            performSearch();
        }
    });
});


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
        
        const seeDetails = () => {
            window.location.href = `/details/${encodeURIComponent(doc.id)}`;
        }

        item.addEventListener('click', seeDetails);
        item.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                seeDetails();
            }
        })

        // Title
        const title = document.createElement('h3');
        title.className = 'result-title';
        title.innerText = cleanText(doc.name);
        item.appendChild(title);

        // Description
        const description = document.createElement('p');
        description.className = 'result-description';
        const rawDesc = doc.description || 'No description available.';
        const descText = Array.isArray(rawDesc) ? rawDesc[0] : rawDesc;
        description.innerText = descText.length > 250 ? descText.substring(0, 250) + '...' : descText;
        item.appendChild(description);

        if (doc.symptoms) {
            const badgeContainer = document.createElement('div');
            badgeContainer.className = 'badges-container';

            badgeContainer.style.display = 'flex';
            badgeContainer.style.alignItems = 'center';
            badgeContainer.style.flexWrap = 'wrap';
            
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
            treatmentHint.innerText = `💊 ${treatmentList.length} Treatment${treatmentList.length !== 1 ? 's' : ''} available.`;
            item.appendChild(treatmentHint);
        }

        resultsContainer.appendChild(item);
    });
}