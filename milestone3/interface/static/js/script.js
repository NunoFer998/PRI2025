document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('search-button').addEventListener('click', performSearch);
    
    document.getElementById('search-input').addEventListener('keypress', function(event) {
        if (event.key === 'Enter') {
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
        item.className = 'result-item';

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

        resultsContainer.appendChild(item);
    });
}