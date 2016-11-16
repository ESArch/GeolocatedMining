## Credit to http://sentistrength.wlv.ac.uk/documentation/Python.txt for the code

import shlex, subprocess


def RateSentiment(sentiString):
    # open a subprocess using shlex to get the command line string into the correct args list format
    p = subprocess.Popen(shlex.split("java -jar /Users/billyyuan/Downloads/SentiStrengthCom.jar stdin sentidata \
                                     /Users/billyyuan/Downloads/Sentstrength_Data_Sept2011/ "),
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # communicate via stdin the string to be rated. Note that all spaces are replaced with +
    stdout_text, stderr_text = p.communicate(sentiString.replace(" ", "+"))
    # remove the tab spacing between the positive and negative ratings. e.g. 1    -5 -> 1-5
    stdout_text = stdout_text.rstrip().replace("\t", "")


print (RateSentiment("I like pie"))