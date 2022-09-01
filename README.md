# Easy Track Platform

Table of contents
=================
<!--ts-->
   * [1. Brief platform overview](#Brief-platform-overview)
   
   * [2. ET gRPC Server functionalities](#ET-gRPC-Server-functionalities)
      * [2.1. User management module](#User-management-module)
      * [2.2. Campaign management module](#Campaign-management-module)
      * [2.3. Data management module](#Data-management-module)
      * [2.4. Communication management module](#Communication-management-module)
      * [2.5. Data processing pipeline](#Data-processing-pipeline)
   * [3. Third-Party App integration](#Third-Party-App-integration)
      * [3.1. Authenticator assistant application](#Authenticator-assistant-application)
      * [3.2. Easy Track Library](#Easy-Track-Library)
      * [3.3. Local-DB management library](#Local-DB-management-library)
      * [3.4. Data submission management library](#Data-submission-management-library)
      * [3.5. Notification management library](#Notification-management-library)
      * [3.6. Direct message management library](#Direct-message-management-library)
      * [3.7. Useful tips (i.e., power consumption, etc.)](#Useful-tips-(i.e.,-power-consumption,-etc.))
   * [4. ET Web Dashboard manual](#ET-Web-Dashboard-manual)
     * [4.1. Authentication](#Authentication)
     * [4.2. Campaign creation, editing and deletion](#Campaign-creation,-editing-and-deletion)
        * [4.2.1. Data source creation](#Data-source-creation)
        * [4.2.2. Finalizing the campaign creation](#Finalizing-the-campaign-creation)
     * [4.3. Campaign editing](#Campaign-editing)
     * [4.4. Campaign monitoring](#Campaign-monitoring)
     * [4.5. Viewing / downloading data](#Viewing-/-downloading-data)
<!--te-->

## Brief platform overview
![alt text](https://github.com/Qobiljon/ET_Dashboard/blob/master/images/et-building-blocks.png)

<p align="center">
    Fig 1. EasyTrack Platform Design 
</p>
 
EasyTrack is a data collection platform for researchers to collect sensing data from mobile / wearable devices and monitor data collection statistics (i.e., missing data, etc). EasyTrack platform consists of four parts, which are: gRPC Server, Web Dashboard, EasyTrack Library for Third-Party apps, and a Third-Party App itself.

EasyTrack gRPC Server is the core part of the platform. It is responsible for storing, and processing data coming from third-party applications, and for providing charts to the web dashboard for monitoring purposes. EasyTrack Web dashboard is a simple (django) web application that retrieves data from gRPC server and visualizes in a browser. The major functionalities of the EasyTrack Library for third-party apps are: Local DB management, Data submission management, Notifications management, and Direct message management. The library helps researchers easily integrate their Third-Party Applications with our platform.

The following sections will guide you through the details about the EasyTrack gRPC server’s functionalities, Third-Party App integration tutorial, and the EasyTrack dashboard manual.

## ET gRPC Server functionalities

EasyTrack server’s RPCs are written in Google’s protobuf format. You can download and play with the RPCs by downloading the protobuf file from [our GitHub repository.](https://github.com/Qobiljon/EasyTrack_proto) The RPCs are served to both Third-Party Applications and [our Web Dashboard.](http://etdb.myvnc.com/) The RPCs are divided into separate modules, and to better understand them, the following subsections explain their functionalities in detail.

### User management module

The user management module provides registration, authentication, and campaign joining functionalities to campaign’s participants that use Third-Party Applications. Also, it separately provides registration, and authentication functionalities to campaign managers (campaigners) that use Web Dashboard.

* Authenticate using Google oAuth2 (third-party apps)
   * This functionality is for third-party applications, that is used for authenticating users with a Google oAuth2 ID token. Registration and authentication functionalities are merged into a single “Authenticate” functionality, as it automatically registers a user if it doesn’t exist in the system. As our assistant EasyTrack Authenticator application handles this functionality, one will not be required to work with this part while integrating a third-party application with EasyTrack platform.
* Google oAuth2 (dashboard)
   * This functionality works in the same way as above, serving the Web Dashboard instead of Third-Party Applications.
* Bind user to campaign
   * By using this functionality, users of Third-Party applications can join a campaign, where the server maps the participant with the specified campaign.

### Campaign management module

The campaign management module provides such functionalities as campaign creation, editing, deletion, and the management of data sources associated with campaigns. This module serves our Web Dashboard, which does not play a role during Third-Party Application integration. The Web Dashboard makes a use of this module to serve campaigners.

* Campaign creation
   * This functionality creates campaign in the server database with the provided details about a campaign (i.e., title, data sources, data source configurations, campaign start and end times, etc.)
* Campaign editing
   * This functionality provides a service to Web Dashboard of modifying details / configurations of already existing campaign details (i.e., adding a data source, changing a sensor’s sampling rate, changing start and end times, etc.)
* Campaign deletion
   * Using this functionality, the Web Dashboard can delete an existing campaign. Note: a campaign can be deleted only from the profile of the campaign’s creator.
* Retrieving all data sources
   * This functionality provides the Web Dashboard all available data sources (i.e., available sensors, surveys, etc.)
* Data source binding / creaton
   * Using this functionality, the Web Dashboard can attach data sources to their campaign. In other words, campaign’s data sources (and their configurations, such as sampling rates) become available for the Third-Party applications. Also, the data source binding functionality will automatically create a new data source and attach it to the campaign if one has not been registered in the server’s database before.

### Data management module

This module is responsible for storing data (i.e., sensor readings, EMAs, etc.), calculating statistics (i.e., DQ:Completeness, LOF, etc.), and providing the data at a request (i.e., for viewing/downloading data on Web Dashboard).
* Data storage (i.e., sensor, EMA, etc.)
   * This functionality is for storing the data when a Third-Party application submits one.
* Retrieving / extracting data (for filtering / downloading)
* Data processing (DQ: completeness, LOF: unexpected abnormal behavior)
   * The data processing pipeline is an always running process that calculates DQ:Completeness, LOF, and other statistics (i.e., participation duration, etc.) for presenting them on the Web Dashboard.

### Communication management module

This module simply provides four functionalities, two of which are for sending/receiving direct messages, and the other two for sending/receiving notifications. The messages and notifications are stored on the server’s database, and it is up to the Third-Party application’s designer to decide how to present the messages and notifications within their applications.
* Storing message (for sending direct message)
   * By using this functionality stores a new direct message record that will be labeled as an ‘unread message’ for the destination user. 
* Retrieving unread messages (for receiving unread direct messages)
   * When this functionality is used, the calling user can retrieve the unread messages directly sent to him/her, if ones are available in the server database.
* Storing a notification
   * This functionality is for a Web Dashboard, that broadcasts a message to all participants that are bound (have joined) to a particular campaign.
* Retrieving an unread notification
   * This functionality is for Third-Party applications to retrieve all unread notifications targeted to their bound (joined) campaign.

### Data processing pipeline

Data processing pipeline is an always running process that calculates statistics related to each campaign, data source, and each participant.
* DQ:Completeness calculation
   * Completeness is calculated using a simple formula:
      * Real amount of samples / Expected amount of samples → (%)
* LOF Calculation (for abnormal behavior detection)
   * Using [LOF](https://dl.acm.org/doi/10.1145/342009.335388), and [LoOP](https://pypi.org/project/PyNomaly/) algorithms
* Other statistics
   * Participation duration
      * Difference between the current day and participant’s day of participation
   * Last sync time
      * The timestamp taken from the last sample that was submitted by a participant
   * Last heartbeat time
      * The last time a participant’s device was online (accessible)


## Third-Party App integration

Third-Party applications can make remote procedure calls to the EasyTrack gRPC server in order to do various actions (i.e., submit sensor data, send a direct message to a campaigner, retrieve EMA, etc.). We provided a library for Third-Party application developers to make the integration much easier, which are explained in detail in this section. Also, a sample app is available for testing via [this link.](https://github.com/Qobiljon/EasyTrack_AndroidAgent) Simply, the steps need to be taken to integrate a third-party app with EasyTrack platform are as follows :
   1. Use our assistant application to authenticate users
   2. Bind the authenticated user to your campaign
   3. Actions of data / communication management modules in any order, i.e.:
       1. Submitting data
       2. Retrieving unread notifications / direct messages
       3. Retrieving data
       4. etc.

### Authenticator assistant application


<p align="center">
  <img src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Authenticator_assistant_application.png">
</p>
<p align="center">
   Fig 2. Authenticating a user from a Third-Party Application
</p>

The EasyTrack authenticator is an assistant application that must be available in target users’ devices. It provides a launchable intent, that you will need to start from your application’s activity. In order to authenticate a user in your application, you must perform the following three simple steps in your source code, following the specified order.

_step 1:_ Ask users to install our assistant application if it doesn’t exist in the user’s device
```kotlin 
override fun onCreate(savedInstanceState: Bundle?) {
   super.onCreate(savedInstanceState);
   setContentView(R.layout.YOUR_ACTIVITY_RES);
   
   val authAppIsNotInstalled = try {
      packageManager.getPackageInfo("inha.nsl.easytrack", 0)
      false
   } catch (e: PackageManager.NameNotFoundException) {
      true
   }
   if (authAppIsNotInstalled) {
      Toast.makeText(this, "Please install this app!", Toast.LENGTH_SHORT).show()
      val intent = Intent(Intent.ACTION_VIEW)
      val appUrl = "https://play.google.com/store/apps/details?id=inha.nsl.easytrack"
      intent.data = Uri.parse(appUrl)
      intent.setPackage("com.android.vending")
      startActivityForResult(intent, RC_OPEN_APP_STORE)
  }
}
```

_Step 2:_ Once the app is installed, starting an intent from the app to authenticate the user
```kotlin
val launchIntent = packageManager.getLaunchIntentForPackage("inha.nsl.easytrack")
if (launchIntent != null) {
   launchIntent.flags = 0
   startActivityForResult(launchIntent, RC_OPEN_ET_AUTHENTICATOR)
}
```
_Step 3:_ Get the result from the assistant application (success / failure, email address, etc.)
```kotlin
override fun onActivityResult(requestCode: Int, resultCode: Int, data: Intent?) {
   if (requestCode == RC_OPEN_ET_AUTHENTICATOR) {
      if (resultCode == Activity.RESULT_OK) {
         if (data != null) {
            // Successful authentication
            val fullName = data.getStringExtra("fullName")
            val email = data.getStringExtra("email")
            val userId = data.getIntExtra("userId", -1)
            // YOUR CODE HERE
         }
      } else if(resultCode == Activity.RESULT_FIRST_USER) {
        // User has canceled the authentication process
      } else if(resultCode == Activity.RESULT_CANCELED) {
        // Failure (i.e., network unavailable, etc.)
      }
   }
   super.onActivityResult(requestCode, resultCode, data)
}
```
### EasyTrack Library

After authenticating a user, you can integrate with the platform easily by adding the Easytrack-Library as a dependency to your application. There are two steps to do this, as in the following figure:
<p align="center">
  <img width="600" height="450" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Easy_Track_library.png"/600/450>
</p>
<p align="center">
   Fig 3. Adding the EasyTrack-Library as a dependency
</p>

### Local-DB management library

Using Local-DB management that comes in the library, you can store and retrieve data (i.e., sensor recordings, EMAs, notifications, direct messages, logs, etc.) in a device.

### Data submission management library

[Empty]

### Notification management library

[ Content ]

### Direct message management library

[ Content ]

## ET Web Dashboard manual

The EasyTrack Web Dashboard is a tool for researchers to manage their data collection campaigns from any device, and keep track of the data collection with simple, yet informative statistics about their campaigns progress. It is accessible via [this link.](http://etdb1.myvnc.com/) The following sections provide instructions and details on how to use our Web Dashboard step-by-step.

### Authentication

In order to start using the web dashboard, you will need to sign in using a Google account by clicking the ‘Login with Google’ button as in the following figure. Your Google account is used only for retrieving an email address and a full name.

<p align="center">
  <img width="600" height="200" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_4_Authentication.png"/600/200>
</p>
<p align="center">
   Fig 4. Authenticating on a Web Dashboard
</p>


<p align="center">
  <img width="600" height="400" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_5_Google_account.png"/600/400>
</p>
<p align="center">
   Fig 5. Picking a Google account
</p>

### Campaign creation, editing and deletion

<p align="center">
  <img width="600" height="180" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_6_Campaign_list.png"/600/180>
</p>
<p align="center">
   Fig 6. Campaign’s list (empty)
</p>

Once you have authenticated, you will be on the main page that contains the list of your campaigns. In the Fig. 5 you can see an empty list of campaigns that you should also see when you open the dashboard for the first time.

<p align="center">
  <img width="600" height="390" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_7_Campaign.png"/600/390>
</p>
<p align="center">
   Fig 7. Campaign creator page
</p>

When you click the ‘Create a new campaign’ button, you will be moved to the campaign creator page (as in the Fig. 6). In the campaign creator page, you can specify the name, start and end times, and the data sources with their configurations (i.e., sampling rate, days of week, etc). Configurations of data sources must be written in a valid json format (key → value), and optionally contain the “delay_ms” field if your data source has a sampling rate (accelerometer, light,  scheduled EMA, etc.). The “delay_ms” is used by the gRPC Server to calculate the DQ:Completeness for your campaign.

#### Data source creation

If you cannot find a data source that you wish to pick, you can create a new data source by clicking the ‘Create a new data source’ button.


<p align="center">
  <img width="550" height="150" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_8_New_data_source.png"/550/150>
</p>
<p align="center">
   Fig 8. New data source creation
</p>
By clicking the button, you will see a new row being attached at the top of the list of data sources. Now you can rename the data source, and write a configuration to it as any other data source.

#### Finalizing the campaign creation

The campaign is finally created by filling out the details (i.e., name, start and end times, etc.) and clicking the ‘Create’ button at the bottom of the campaign creator page.

<p align="center">
  <img width="500" height="400" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_9_Data_source_selection.png"/500/400>
</p>
<p align="center">
   Fig 9. Data source selection
</p>

<p align="center">
  <img width="500" height="100" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_10_Campaign_creation.png"/500/100>
</p>
<p align="center">
   Fig 10. Campaign creation (by clicking ‘Create’ button)
</p>

### Campaign editing

Once you have created a campaign, you can click the ‘Edit’ button on your campaign from the list, and go to a campaign editing page.
<p align="center">
  <img width="500" height="200" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_11_Edit_button.png"/500/200>
</p>
<p align="center">
   Fig 11. Clicking ‘Edit’ button on a campaign from campaigns list
</p>

<p align="center">
  <img width="500" height="300" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_12_Editing.png"/500/300>
</p>
<p align="center">
   Fig 12. Editing an existing campaign
</p>

### Campaign monitoring

[ TBD ]

### Viewing / downloading data

<p align="center">
  <img width="500" height="120" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_13.png"/500/120>
</p>
<p align="center">
   Fig 13. Picking a participant to view/download their data
</p>

From the list of your campaign’s participants, click any participant to view or download their data as in the Fig. 12.

<p align="center">
  <img width="500" height="250" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_14.png"/500/250>
</p>
<p align="center">
   Fig 14. Clicking ‘View’ button on a specific data source to view the data
</p>

<p align="center">
  <img width="500" height="300" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_15.png"/500/300>
</p>
<p align="center">
   Fig 15. Viewing raw data (100 samples at a time)
</p>

<p align="center">
  <img width="500" height="100" src="https://github.com/Qobiljon/ET_Dashboard/blob/master/images/Fig_16.png"/500/100>
</p>
<p align="center">
   Fig 16. Clicking ‘Download’ button to download the complete data (of a specific data source)
</p>

