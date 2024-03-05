
# Flask Application Documentation

## Overview

This document outlines the structure and functionality of a Flask-based web application designed for movie database exploration and list management. The application provides several endpoints for different functionalities, including searching movies, managing user-saved lists, and user authentication.

### Database from KODI with all the videos
https://kodi.wiki/view/Databases/MyVideos

## Application Setup

### Routes

- `/`: The home route renders the main page of the application.
- `/genres`: Returns a list of movie genres from the database.
- `/save_list`: Allows users to save a list of movies.
- `/fetch_saved_list_items`: Retrieves movies from a user's saved list.
- `/get_saved_list_names`: Gets the names of the saved lists for the logged-in user.
- `/search`: Performs a search based on the provided criteria (movie name, actor name, director name, etc.).
- `/login/users`: Handles user login functionality.

### Key Functions

- **`genres()`**: Loads a SQL query from a file to retrieve genres and executes it.
- **`save_list()`**: Saves a list of movie IDs for a user.
- **`get_saved_list_items()`**: Fetches movies based on a saved list's movie IDs.
- **`get_saved_list_names()`**: Returns the names of lists saved by the user.
- **`search()`**: Searches the database for movies based on various filters.
- **`login()`**: Authenticates a user against the database.

## JavaScript Functions

- **`downloadProcessedDataAsJson()`**: Downloads the processed data as a JSON file.
- **`_saveList()`**: Internal function to save a list of movies.
- **`saveListAs()`**: Prompts the user for a list name and saves the list.
- **`saveUpdatedList()`**: Saves the currently selected list with updates.
- **`fetchAndDisplayListItems()`**: Fetches and displays items from a saved list.
- **`_displaySavedListItems()`**: Internal function to display saved list items.
- **`authenticateUser()`**: Handles user authentication.
- **`_populateUserListsDropdown()`**: Populates the dropdown with user's saved lists.
- **`_displayLoginResults()`**: Displays the result of a login attempt.
- **`executeSearch()`**: Executes a search based on user input.
- **`_displaySearchResults()`**: Displays the search results.
- **`removeSelectedFromProcessedList()`**: Removes selected items from the processed list.
- **`addToProcessedList()`**: Adds selected items to the processed list.
- **`populateGenreDropdown()`**: Populates the genre dropdown from the database.
- **`selectAll()`**: Selects all items in the search results.
- **`selectNone()`**: Deselects all items in the search results.
- **`clearList()`**: Clears the processed list.
- **`on_load_do_things()`**: Function called on window load to initialize certain UI elements.

## Getting Started

To run this application, ensure you have Flask installed in your environment, then navigate to the directory containing `app.py` and execute:

```bash
python app.py
```

This will start the Flask development server on port 3200.

## Usage

Interact with the application via its web interface or directly through its endpoints using tools like `curl` or Postman.

## Note

This application is a demonstration and should be properly secured and configured before deploying to a production environment.
