# 6.830-project

We recommend that a python virtual environment is created in the directory for this project to run the code. Simple setup instructions are below, but you can [skip to how to run our code](#stream) if prefer. 

<b id="venv">Adding a virtual environment (on Mac).</b>  
Navigate to the root directory of this project in terminal.  Install the venv package if it isn't already:  
```bash
$ pip install venv
```  
Then create the virtual environment and activate it:
```bash
$ virtualenv venv
$ source venv/bin/activate
```  
Finally, install the relevant packages:
```bash
$ pip install -r requirements.txt
```


<b id="stream">To directly run the streaming module in the terminal:</b>  
Switch to the <i>master</i> branch.  In the command line, type `python two_streams.py`, and an output of dictionaries with both tweet and meetup data should appear relatively quickly in the terminal's output.

<b id="window">To run isolated test cases for the Window module:</b> 

Open window_class.py in your favorite code editor, and uncomment the docstrings with the test cases, or create your own.  (Line 146 must be commented after its enclosing docstring is removed.)  Note that an Aggregator instance doesn't take care of determining what values it holds, as this responsibility falls to the Window class; also note the lambda functions provided in line 147 calculate an incremental average. 

When you're ready, run `python window_class.py`.  

<b id="visualization">To directly run the visualization component:</b>  
Switch to the <i>marie</i> branch.  In the command line, type `python app.py`.  This will open up the visualization page at [localhost:5000](localhost:5000); however, the map takes a while to start populating as input data is limited by the Meetup stream, which is relatively slow.
