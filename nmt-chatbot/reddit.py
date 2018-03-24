import praw
import modded as modin #Import file for bot inputs/replys

bot = praw.Reddit(user_agent='HAFARChatbot',
                  client_id='MGrVVdzfCb2xsw',
                  client_secret='ZGejvhDkQnF6W_bJkBU6x7nLymM',
                  username='IAnAtoN',
                  password='BotAccountPassword321!') #Login using bot account and app settings


subreddit = bot.subreddit('IAnAtoN') #Subreddit for posting

print("Logged in, found subreddit")

comments = subreddit.stream.comments()

for comment in comments:
    try:
        #for comment in comments:
        text = comment.body
        author = comment.author

        message = "unkown"
        if author != "IAnAtoN": #Only reply to people other than the bot itself
            message = modin.mainFunction(text.lower())

            print("""Original comment - {}
                        (author - u/{},
                        subreddit - r/{})\n
                        Reply - {} \n\n""".format(text, author, subreddit, message)) #Terminal output of what's happening

            comment.reply(message + """\n \n------------------------------------- \n \nThis is a bot, it's stil an idiot. Check it out at r/IAnAtoN""") #Post comment reply on Reddit
    except praw.exceptions.APIException as e:
            print("Exception found")
