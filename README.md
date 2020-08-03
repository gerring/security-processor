# Introduction
 A project which injests security data and stores it so that uit can be used in a performant manner.
 
# Setup
Add src and test to the python path in IDE. If using pytdev this will be automatic.
Click on test.py and run as python unit test. 

# Main Code
The lambda is currently in src/com/stockopedia/securities/lambda.py and UNFINISHED
The Transformer is in src/com/stockopedia/securities/engine.py there are unit tests for some of its actions, many more are possible

# TODO
     
    0. Many tests required.
    1. Check process method of list of items.
    2. More validation around timestamps being legal. 
    3. Check security code against list of supported codes?
    4. Write lambda for DynamoDB queries, check speed.
    5. Much else besides.
