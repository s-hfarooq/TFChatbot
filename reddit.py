import praw
import modded as modin #Import file for bot inputs/replys
import atexit
import numpy
import time



postComment = True #True means it posts the comment to Reddit
botPass = 'ENTER_PASSWORD'

bot = praw.Reddit(user_agent = 'HAFARChatbot',
                  client_id = 'MGrVVdzfCb2xsw',
                  client_secret = 'ZGejvhDkQnF6W_bJkBU6x7nLymM',
                  username = 'IAnAtoN',
                  password = botPass) #Login using bot account and app settings



def replyToComments(subreddit):
    subreddit = bot.subreddit(subreddit)
    print("Logged in, found subreddit")

    comments = subreddit.stream.comments()
    replied_to = []


    for comment in comments:
        try:
            text = comment.body
            author = comment.author

            text_file = "commentsRepiedTo.txt" #File with comment ID's
            readFile = open("commentsRepiedTo.txt", "r")
            read_in = readFile.read().split(", ")

            message = "unkown"
            if comment.id not in read_in:
                if author != "IAnAtoN": #Only reply to people other than the bot itself
                    time.sleep(30)
                    message = modin.mainFunction(text.lower())
                    messageClean = message
                    messageClean = message.replace("\\bnewlinechar\\b","\n\n")
                    replied_to.append(comment.id)

                    writeFile = open("commentsRepiedTo.txt", "a") #Writes new comment ID to file
                    writeFile.write(comment.id + ", ")


                    print("""Original comment - {}
                                (author - u/{},
                                subreddit - r/{})\n
                                Reply - {} \n\n""".format(text, author, subreddit, messageClean)) #Terminal output of what's happening


                    if(postComment == True):#Post comment reply on Reddit if desired
                        comment.reply(messageClean + """\n\n------------------------------------- \n\nThis is a bot, it's stil an idiot. Check it out at r/IAnAtoN""")


            else:
                print("Already repied to comment: ", text, ".... Moving on")
        except praw.exceptions.APIException as e:
                print("Exception found: ", str(e))


def exit_handler():
    readFile.close()
    writeFile.close()
    print("Commet id's replied to: ", replied_to)


replyToComments('IAnAtoN') #Post in r/IAnatoN


atexit.register(exit_handler)
