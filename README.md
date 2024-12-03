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
   [here](<PowerApp/Images/flow1.png>)
  
- Use the Create file action to save the attachment to your OneDrive in the folder named **YelloTaxi**.
 [here](<PowerApp/Images/createfile.png>)

### Data Transformation and Load
- Add a Power Query action to load the data from the OneDrive file.

[here](<PowerApp/Images/before_transform.png>)
- This represents a view of the data after it has been successfully loaded from onedrive.
- 
### Implement the following transformations:
- Remove unnecessary columns: Use the relevant Power Query option.
- Filter out rows: Set a condition to exclude rows with drop-off coordinates equal to 0.
- Parse coordinates: Convert coordinates to text format for compatibility.
  [here](<PowerApp/Images/YellowProfile.png>)

### Data Storage in Dataverse
- Use the Add a row action to load the transformed data into a Dataverse table named TaxiData.
[here](<PowerApp/Images/add_new_row.png>)

### Data Profiling
- Create a new Dataverse table named TaxiDataProfile.

 ### Extract and store the following profiles:
- Count of unique records.
- Number of errors during transformation.
- Summary of transformations performed.
  [here](<PowerApp/Images/taxiprofile.png>)

### Notification
- Add a final action to send an email notification when the data is refreshed in Dataverse, including an HTML table of the data profile.
[here](<PowerApp/Images/complete_flow.png>)

## 3. PowerApps Canvas App Creation

### Create a Canvas App

In PowerApps, select your DE TaxiDemo solution.

- Click on New and choose Canvas App.
- Visualize Taxi Locations
- Connect the app to the TaxiData table.
- Use a map control to display taxi locations.
- Add additional controls to show relevant taxi data.
[here](<PowerApp/Images/YellowTaxiApp.png>)

### User Interface
Design a clean and user-friendly interface using Power Apps design features.
