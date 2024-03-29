var selectedProcessedItems = []
window.processedItemsData = [];
var selectedSearchItems = []

function downloadProcessedDataAsJson() {
    console.log('Downloading processed data as JSON');
    console.log(window.processedItemsData);

    const dataStr = JSON.stringify(window.processedItemsData, null, 4); // The 'null' and '4' arguments format the JSON for readability
    console.log(dataStr);

    // Create a Blob object representing the data as a JSON file
    const blob = new Blob([dataStr], { type: "application/json" });
    // Create a temporary anchor element
    const a = document.createElement('a');
    // Create a URL for the blob object
    const url = URL.createObjectURL(blob);
    // Set the href attribute of the anchor to the blob URL
    a.href = url;

    // get list name from the dropdown and timestamp
    var listName = document.getElementById('saved-lists-dropdown').value;
    var today = new Date();
    var date = today.getFullYear() + '-' + (today.getMonth() + 1) + '-' + today.getDate();
    var hours = today.getHours().toString().padStart(2, '0');
    var minutes = today.getMinutes().toString().padStart(2, '0');
    var seconds = today.getSeconds().toString().padStart(2, '0');
    var dateTime = date + '_' + hours + '-' + minutes + '-' + seconds;

    // Set the download attribute to the desired file name
    console.log('listName:', listName);
    console.log('dateTime:', dateTime);
    a.download = `${listName}_${dateTime}.json`;

    // Append the anchor to the document
    document.body.appendChild(a);

    // Trigger the download by simulating a click on the anchor
    a.click();
    // Clean up by removing the anchor and revoking the blob URL
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}
    
function _saveList(list_name) {
    // Capture input values
    var listName = list_name;
    movie_ids_mapped = window.processedItemsData.map(item => item.idMovie);

    console.log('Selected list for saving:', listName);
    console.log('Selected items for saving:', movie_ids_mapped);

    // Construct the data object to send
    var listData = {
        list_name: listName,
        movie_ids: movie_ids_mapped
    };

    fetch('/save_list', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(listData),
    })
    .then(response => {
        if (!response.ok) {
            // Throw an error with a message that includes the status code, if needed
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return response.json(); // We only parse the JSON if the response was ok.
    })
    .then(data => {
        console.log('Success with saveUpdatedList:', data);
        // Only alert success here, as this block is reached only if there were no errors
        alert('List saved successfully');
    })
    .catch((error) => {
        console.error('Error:', error);
        // Handle all errors in the .catch block, including network errors and thrown errors
        alert('Error saving list. Please try again.');
    });

    // Refresh the dropdown with saved lists
    _populateUserListsDropdown();
}

function saveListAs() {
    var listName = ""; // Initialize listName outside the loop
    var isValidName = false; // Flag to check if the name is valid

    // Retrieve existing list names from the dropdown
    const savedListsDropdown = document.getElementById('saved-lists-dropdown');
    const existingListNames = Array.from(savedListsDropdown.options).map(option => option.value);

    // Keep asking for input until isValidName becomes true
    while (!isValidName) {
        listName = prompt("Please enter the name of the new list", "New List");
        if (listName != null) {
            if (listName.trim() === '') {
                alert("List name cannot be empty, please enter a valid name");
            } else if (listName.length > 20) {
                alert("List name is too long, please enter a name with less than 20 characters");
            } else if (listName.match(/[^a-zA-Z0-9 ]/)) {
                alert("List name contains special characters, please enter a name with only letters and numbers");
            } else if (existingListNames.includes(listName)) {
                alert("List name already exists, please enter a unique name");
            } else {
                isValidName = true; // Set the flag to true if all conditions are met
            }
        } else {
            // Break out of the loop if the user cancels the prompt
            return;
        }
    }

    // Once a valid name is entered, proceed with the rest of the function
    if (isValidName) {
        _saveList(listName);
    } else {
        // Handle the case where the user cancels the prompt
        alert('List not saved');
    }
}

function saveUpdatedList() {
    // Capture input values
    var listName = document.getElementById('saved-lists-dropdown').value;
    try {
        _saveList(listName)
    } catch (error) {
        console.error('Error:', error);
        alert('Please select a list to save');
    }
}

function fetchAndDisplayListItems() {
    // Capture input values
    var listName = document.getElementById('saved-lists-dropdown').value;
    console.log('Selected list:', listName);

    // Construct the data object to send
    var listData = {
        list_name: listName
    };

    // Make an AJAX request to the Flask backend
    fetch('/fetch_saved_list_items', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(listData),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Success with fetchAndDisplayListItems:', data);
        _displaySavedListItems(data);
    })
    .catch((error) => {
        console.error('Error:', error);
    });
}

function _displaySavedListItems(data) {
    // Get the element where the saved list items will be displayed
    console.log('_displaySavedListItems() Saved list items:', data);

    // Ensure window.processedItemsData is initialized
    window.processedItemsData = []

    // Update window.processedItemsData with new items, checking for duplicates
    data.forEach(function(item) {
        const isItemProcessed = window.processedItemsData.some(processedItem => processedItem.idMovie === item.idMovie);
        if (!isItemProcessed) {
            window.processedItemsData.push(item);
        }
    });
        
    // Deduplicate window.processedItemsData
    window.processedItemsData = window.processedItemsData.filter((item, index, self) =>
        index === self.findIndex((t) => t.idMovie === item.idMovie)
    );

    // Sort window.processedItemsData
    window.processedItemsData.sort((movieA, movieB) => movieA.title_formatted.localeCompare(movieB.title_formatted));

    // Update the DOM based on the unique items in window.processedItemsData
    var processedListElement = document.getElementById('processed-list');
    processedListElement.innerHTML = ''; // Clear existing items

    window.processedItemsData.forEach(function(item, index) {
        var li = document.createElement('li');
        var checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = 'saved-item-' + index;
        checkbox.value = index;
        li.appendChild(checkbox);

        // Reintroduce the event listener for each checkbox
        checkbox.addEventListener('change', function() {
            if (this.checked) {
                // Add to the selectedProcessedItems array if checked
                selectedProcessedItems.push(item);
            } else {
                // Remove from the selectedProcessedItems array if unchecked
                selectedProcessedItems = selectedProcessedItems.filter(selectedItem => selectedItem.idMovie !== item.idMovie);
            }
            console.log(selectedProcessedItems);
        });

        var text = document.createTextNode(`${item.title_formatted} (ID: ${item.idMovie})`);
        li.appendChild(text);
        processedListElement.appendChild(li);
    });
}

function authenticateUser() {
    // Capture input values
    var userLogin = document.getElementById('user_login').value;
    var userPassword = document.getElementById('user_password').value;
    
    //show in the console the username/password
    console.log("starting authenticateUser")
    console.log("These are the values of the user login and password")
    console.log(userLogin);
    console.log(userPassword);

    // Construct the data object to send
    var loginData = {
        user_login: userLogin,
        user_password: userPassword
    };

    // Make an AJAX request to the Flask backend
    fetch('login/users', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(loginData),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Success:', data);
        if (data['result'] === true) {
            logged_in_username = data['record']['username']
            sessionStorage.setItem('username', logged_in_username)
            console.log('Logged in username:', logged_in_username);
        }
        // Call a function to handle the display of search results
        _displayLoginResults(result = data['result']);

        // Now that the user is logged in, fetch and populate saved lists 
        _populateUserListsDropdown();
    })
    .catch((error) => {
        console.error('Error:', error);
    });
}

function _populateUserListsDropdown() {
    fetch('/get_saved_list_names', {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        }
    })
    .then(response => response.json())
    .then(lists => {
        const savedListsDropdown = document.getElementById('saved-lists-dropdown');
        savedListsDropdown.innerHTML = ''; // Clear existing options

        if (lists.length === 0) {
            // Handle case where no lists are returned
            const option = document.createElement('option');
            option.value = 'No lists saved';
            option.text = 'No lists saved';
            savedListsDropdown.appendChild(option);
            console.log('No lists saved');
        } else {
            // Populate the dropdown with saved lists
            lists.forEach(list => {
                const option = document.createElement('option');
                option.value = list.list_name; 
                option.text = list.list_name;
                savedListsDropdown.appendChild(option);
                console.log('Saved list:', list.list_name);
            });
        }

        // Event listener to track selected items
        savedListsDropdown.addEventListener('change', function() {
            const selectedList = this.options[this.selectedIndex];
            console.log(`Selected list: ${selectedList.text} (ID: ${selectedList.value})`); 
        });
    })
    .catch(error => {
        console.error('Error fetching saved lists:', error);
        // Consider updating the UI to reflect the error state
    });
}

function _displayLoginResults(result) {
    // Get the element where the login status message will be displayed
    var loginStatusMessage = document.getElementById('login_status_message');

        console.log("starting _displayLoginResults")
        console.log(result)
        console.log(typeof result)

        // Check if the login was successful or not and set the message accordingly
        if (result === true) {
            console.log('Result: True, Login successful');
            loginStatusMessage.textContent = "Login sucessful!"; // Set the text content to the message from the server
            loginStatusMessage.style.color = 'green'; // Optional: Change text color for successful login
        } else if (result === false) {
            console.log('Result: False, Login failed');
            loginStatusMessage.textContent = "Login failed, use 'visitor' as username iuw"; // Set the text content to a default message
            loginStatusMessage.style.color = 'red'; // Optional: Change text color for failed login
        } else {
            console.log('Result: Error, An error occurred');
            loginStatusMessage.textContent = 'An error occurred';
            loginStatusMessage.style.color = 'red'; // Optional: Change text color for other errors
        }
}

function executeSearch() {
    // Capture input values
    var movieName = document.getElementById('movie_name').value;
    var actorName = document.getElementById('actor_name').value;
    var directorName = document.getElementById('director_name').value;
    var genreId = document.getElementById('genre').value;
    var yearMin = document.getElementById('year_min').value;
    var yearMax = document.getElementById('year_max').value;

    // Construct the data object to send
    var searchData = {
        movie_name: movieName,
        actor_name: actorName,
        director_name: directorName,
        genre_id: genreId,
        year_min: yearMin,
        year_max: yearMax
    };

    // Make an AJAX request to the Flask backend
    fetch('/search', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(searchData),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Success:', data);
        // Call a function to handle the display of search results
        _displaySearchResults(data);
    })
    .catch((error) => {
        console.error('Error:', error);
    });
}

function _displaySearchResults(results) {
    var resultsList = document.getElementById('results-list');
    resultsList.innerHTML = '';

    results.forEach(function(result, index) {
        var li = document.createElement('li');

        // Create a checkbox for each result
        var checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.id = 'result-' + index;  // Unique ID for each checkbox
        checkbox.value = index;  // Use index or any unique identifier from your result

        // Optional: Store the entire result object as a data attribute
        // This can be useful if you want to use other properties of the result later
        checkbox.dataset.result = JSON.stringify(result);

        // Event listener to track selected items
        checkbox.addEventListener('change', function() {
            if (this.checked) {
                selectedSearchItems.push(result);  // Add to the selected items list
            } else {
                // Remove from the selected items list if unchecked
                selectedSearchItems = selectedSearchItems.filter(item => JSON.stringify(item) !== JSON.stringify(result));
            }
            console.log(selectedSearchItems);
        });

        // Append the checkbox and result text to the list item
        li.appendChild(checkbox);
        var text = document.createTextNode(`${result.title_formatted} (ID: ${result.idMovie})`)
        li.appendChild(text);

        // Append the list item to the results list
        resultsList.appendChild(li);
    });
}

function removeSelectedFromProcessedList() {
// Find all checkboxes in the processed list
var checkboxes = document.querySelectorAll('#processed-list input[type="checkbox"]');

// Filter out the checked ones and map to their corresponding idMovie
var checkedIds = Array.from(checkboxes)
    .filter(checkbox => checkbox.checked)
    .map(checkbox => window.processedItemsData[checkbox.value].idMovie);

// Now, use these ids to filter out the selected items from window.processedItemsData
window.processedItemsData = window.processedItemsData.filter(item => !checkedIds.includes(item.idMovie));

// Update the DOM after removal
var processedListElement = document.getElementById('processed-list');
processedListElement.innerHTML = ''; // Clear existing processed items before redisplay

window.processedItemsData.forEach(function(item, index) {
    var li = document.createElement('li');

    // Create a checkbox for each list item
    var checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.id = 'saved-item-' + index;
    checkbox.value = index; // Assign the index in processedItemsData as the value
    li.appendChild(checkbox);

    // Append the checkbox and result text to the list item
    var text = document.createTextNode(`${item.title_formatted} (ID: ${item.idMovie})`);
    li.appendChild(text);

    // Append the list item to the results list
    processedListElement.appendChild(li);
});
}

function addToProcessedList() {
    var username = sessionStorage.getItem('username', 'not authenticated'); //TODO Does this need to be here?
    
    console.log('Processing selected items:')
    console.log(selectedSearchItems);

    // Ensure window.processedItemsData is initialized
    if (!window.processedItemsData) {
        window.processedItemsData = [];
    }

    // Process selected items and update window.processedItemsData
    selectedSearchItems.forEach(function(item) {
        
        // Check if item is already in the processedItemsData based on idMovie
        const isItemProcessed = window.processedItemsData.some(processedItem => processedItem.idMovie === item.idMovie);

        if (!isItemProcessed) {
            // Prepare item data
            let listItem = {
                idMovie: item.idMovie,
                idPath: item.idPath,
                release_date: item.release_date,
                release_year: item.release_year,
                strPath: item.strPath,
                title_formatted: item.title_formatted,
            };

            // Add new unique item to window.processedItemsData
            window.processedItemsData.push(listItem);
        }
    });

    // Deduplicate window.processedItemsData
    window.processedItemsData = window.processedItemsData.filter((item, index, self) =>
        index === self.findIndex((t) => t.idMovie === item.idMovie)
    );

    // Sort window.processedItemsData if needed
    window.processedItemsData.sort((movieA, movieB) => movieA.title_formatted.localeCompare(movieB.title_formatted));

    // Update the DOM based on the unique items in window.processedItemsData
    var processedListElement = document.getElementById('processed-list');
    processedListElement.innerHTML = ''; // Clear existing processed items before redisplay

    window.processedItemsData.forEach(function(item, index) {
        var li = document.createElement('li');

        // Create a checkbox for each saved list item
        var checkbox = document.createElement('input');
        checkbox.checked = false;
        checkbox.type = 'checkbox';
        checkbox.id = 'saved-item-' + index;
        checkbox.value = index;
        li.appendChild(checkbox);

        // Append the checkbox and result text to the list item
        var text = document.createTextNode(`${item.title_formatted} (ID: ${item.idMovie})`);
        li.appendChild(text);

        // Append the list item to the results list
        processedListElement.appendChild(li);
    });
}

function populateGenreDropdown() {
    // Make an AJAX request to the Flask backend to get the list of genres
    fetch('/genres', {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json',
        }
    })
    .then(response => response.json())
    .then(genres => {
        const genreSelect = document.getElementById('genre');
        genres.forEach(genre => {
            const option = document.createElement('option');
            option.value = genre.id;
            option.text = genre.name;
            genreSelect.appendChild(option);
        });

        // Event listener to track selected items
        genreSelect.addEventListener('change', function() {
            // 'this' refers to the genreSelect element
            const selectedGenre = this.options[this.selectedIndex];
            // Log the genre and id in a single line
            console.log(`Selected genre: ${selectedGenre.text} (ID: ${selectedGenre.value})`); 
        });
    })
    .catch(error => console.error('Error fetching genres:', error));
}

function selectAll() {
    // Implement the logic to select all checkboxes in the results list.
    const checkboxes = document.querySelectorAll('.results-list input[type="checkbox"]');
    checkboxes.forEach(checkbox => {
        checkbox.checked = true;
        // Assuming result data is stored in checkbox.dataset.result
        const result = JSON.parse(checkbox.dataset.result);
        // Check if the item is not already in selectedSearchItems before adding
        if (!selectedSearchItems.some(item => JSON.stringify(item) === JSON.stringify(result))) {
            selectedSearchItems.push(result);
        }
    });
}

function selectNone() {
    // Implement the logic to select all checkboxes in the results list.
    const checkboxes = document.querySelectorAll('.results-list input[type="checkbox"]');
    checkboxes.forEach(checkbox => {
        checkbox.checked = false;
        // Assuming result data is stored in checkbox.dataset.result
        const result = JSON.parse(checkbox.dataset.result);
        // Remove from the selectedSearchItems list if unchecked
        selectedSearchItems = selectedSearchItems.filter(item => JSON.stringify(item) !== JSON.stringify(result));
    });
}

function clearList() {
    // Clear the processed list and the selectedProcessedItems array
    var processedListElement = document.getElementById('processed-list');
    processedListElement.innerHTML = '';
    selectedProcessedItems = [];
    window.processedItemsData = [];
}

function on_load_do_things() {
    populateGenreDropdown();
}

window.onload = on_load_do_things;