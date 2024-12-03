## 1. Environment Setup in Power Apps
### Create a Power Apps Environment
- Sign in to Power Apps.
- Navigate to the Environment section.
- Click on **New** and create an environment named **DE Lecture**.

### Create a Solution

- Within the DE Lecture environment, go to the Solutions tab.
- Click on **New Solution**.
- Name the solution **DE Taxi Demo** and provide a description if needed.

## 2. Power Automate Flow Creation

### Create a New Flow

- Open Power Automate and navigate to the Solutions section.
- Select your DE Taxi Demo solution and click on New Flow.

### Email Processing
- Choose the **When a new email arrives* trigger.
- Set conditions to trigger only when the email body or subject contains **"YellowTaxi"**.
- Add an action to Get attachments and select the email.
- Use the Create file action to save the attachment to your OneDrive in the folder **amed DE PowerAutomate**.

### Data Transformation and Load
- Add a Power Query action to load the data from the OneDrive file.

### Implement the following transformations:
- Remove unnecessary columns: Use the relevant Power Query option.
- Filter out rows: Set a condition to exclude rows with drop-off coordinates equal to 0.
- Parse coordinates: Convert coordinates to text format for compatibility.

### Data Storage in Dataverse
- Use the Add a row action to load the transformed data into a Dataverse table named TaxiData.

### Data Profiling
- Create a new Dataverse table named TaxiDataProfile.
- Extract and store the following profiles:
- Count of unique records.
- Number of errors during transformation.
- Summary of transformations performed.

### Notification
- Add a final action to send an email notification when the data is refreshed in Dataverse, including an HTML table of the data profile.

## 3. Power Apps Canvas App Creation

### Create a Canvas App

In Power Apps, select your DE Taxi Demo solution.

- Click on New and choose Canvas App.
- Visualize Taxi Locations
- Connect the app to the TaxiData table.
- Use a map control to display taxi locations.
- Add additional controls to show relevant taxi data.

### User Interface
Design a clean and user-friendly interface using Power Apps design features.

## 4. Deliverables

###Git Repository
Initialize a Git repository on your local machine.
Add and commit the following files:
Power Automate Flow file.
Power Apps Canvas App configuration files.
Details of the Dataverse schema.
Images
Take screenshots of:
The created Power Apps Canvas App.
The Power Automate Flow setup.
Ensure these images are clear and well-captured.
Final Steps
Review your work to ensure all tasks are completed.
