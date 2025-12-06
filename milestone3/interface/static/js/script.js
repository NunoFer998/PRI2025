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
        resultsContainer.innerHTML = '<h2>No Results Found</h2><p>Try a different symptom or disease.</p>';
        return;
    }

    resultsContainer.innerHTML = `<h2>Found ${numFound} Result(s)</h2>`;

    docs.forEach(doc => {
        const item = document.createElement('div');
        item.className = 'result-item';

        // Title
        const title = document.createElement('h3');
        title.className = 'result-title';
        title.textContent = doc.name || 'Untitled Disease'; 
        item.appendChild(title);

        // ID
        const id = document.createElement('p');
        id.className = 'result-field';
        id.innerHTML = `<strong>ID:</strong> ${doc.id || 'N/A'}`;
        item.appendChild(id);

        // Symptoms
        const symptoms = document.createElement('p');
        symptoms.className = 'result-field';
        const symptomsText = Array.isArray(doc.symptoms) ? doc.symptoms.join(', ') : (doc.symptoms || 'None listed');
        symptoms.innerHTML = `<strong>Symptoms:</strong> ${symptomsText}`; 
        item.appendChild(symptoms);

        // Description
        const description = document.createElement('p');
        description.textContent = doc.description ? doc.description.substring(0, 200) + '...' : 'No description available.';
        item.appendChild(description);

        resultsContainer.appendChild(item);
    });
}