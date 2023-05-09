import io
import os
import re
import time
import uuid
from dotenv import load_dotenv


import openai
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.helpers import escape_markdown
from telegram.constants import ChatAction, ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, ConversationHandler, MessageHandler, filters, \
    CallbackQueryHandler
import json
import atexit
import signal
import psycopg2
import csv
import tiktoken

# Load environment variables from .env file
load_dotenv()

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
MODEL = os.getenv("MODEL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

CHAT_FILE = 'data_test.json'

def num_tokens_from_messages(messages, model="gpt-4"):
    """Returns the number of tokens used by a list of messages."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        # print("Warning: model not found. Using cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    if model == "gpt-3.5-turbo":
        print("Warning: gpt-3.5-turbo may change over time. Returning num tokens assuming gpt-3.5-turbo-0301.")
        return num_tokens_from_messages(messages, model="gpt-3.5-turbo-0301")
    elif model == "gpt-4":
        # print("Warning: gpt-4 may change over time. Returning num tokens assuming gpt-4-0314.")
        return num_tokens_from_messages(messages, model="gpt-4-0314")
    elif model == "gpt-3.5-turbo-0301":
        tokens_per_message = 4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
        tokens_per_name = -1  # if there's a name, the role is omitted
    elif model == "gpt-4-0314":
        tokens_per_message = 3
        tokens_per_name = 1
    else:
        raise NotImplementedError(f"""num_tokens_from_messages() is not implemented for model {model}. See https://github.com/openai/openai-python/blob/main/chatml.md for information on how messages are converted to tokens.""")
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 2  # every reply is primed with <im_start>assistant
    return num_tokens



def cleanup():
    print('Emergency saving')
    with open(CHAT_FILE, 'w') as fp:
        json.dump(users_chats, fp)
    print('Emergency saving done')


SYSTEM_PROMPT = """
<|im_start|>system
## You are ChatGPT, a chat bot that can query an  postgres database. Answer as concisely as possible.
* if user's task requires a query, run the query and present the results.
* If user asks for something that doesn't exist, try to find a similar thing in the database by querying for it.
* If possible, print ids of the results.
* You can do up to 3 queries per user message.
* You should format the results of your queries to be human readable and easy to understand.
* You should always do a #(inner_monologue) after you do get a query result #(query_result).
* You should always do a #(message) after you do consider you have enough information to answer the user.
* You should try to fix on #(query_error) and do a #(inner_monologue) about it and then try again.
* If column type is jsonb, you should recursively query for the keys in the jsonb column using `jsonb_object_keys`.
* Always ensure json keys are existent before querying for them.
* Limit your query to **5 rows maximum**. 
* You can use markdown v2 to format your answers.
* You must convert timestamps to human readable format using postgres functions.
* Отвечай на русском языке.

## Example queries
SELECT SUM((elements->'sheet_run_quantity')::integer) as total_sheet_runs FROM bigtuner_digital, jsonb_array_elements(press->'types') as elements;

Current date: 2023-04-02<|im_end|>
"""

if not os.path.exists(CHAT_FILE):
    with open(CHAT_FILE, 'r') as fp:
        users_chats = json.loads(fp.read())
else:
    users_chats = {}

INNER_MONOLOGUE = "#(inner_monologue)"
MESSAGE = "#(message)"
QUERY = "#(query)"
QUERY_RESULT = "#(query_result)"
QUERY_ERROR = "#(query_error)"


def default_prompt():
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "system", "name": "example_user", "content": "#(message)\nHi, list all tables in the database\n"},
        {"role": "system", "name": "example_assistant",
         "content": "#(inner_monologue)\nUser wants to see all tables in the database. I will need to run a query to "
                    "get the list of tables.\n"},
        {"role": "system", "name": "example_assistant",
         "content": "#(query)\nSELECT table_nme FROM information_schema.tables WHERE table_schema = 'public';\n"},
        {"role": "system", "name": "example_assistant",
         "content": "#(query_error)\n```\nError: table with name \"table_nme\" doesn't exists.\n```\n"},
        {"role": "system", "name": "example_assistant",
         "content": "#(inner_monologue)\ntable with name \"table_nme\" doesn't exists. Looks like a typo\n"},
        {"role": "system", "name": "example_assistant",
         "content": "#(query)\nSELECT table_name FROM information_schema.tables WHERE table_schema = 'public';\n"},
        {"role": "system", "name": "example_assistant",
         "content": "#(query_result)\n```\n{{query result here}}\n```\n"},
        {"role": "system", "name": "example_assistant", "content": """#(message)\nSure, here are the tables: 
```
digital_logs, fb_users,
delivery_departments, delivery_requests, paper_price_records, paper_change_records, projects, schema_migrations, 
papers, digital, stages, users, projects_users, users_otp, users_passwords, users_telegram.
```\n
"""},
        {"role": "system", "name": "example_user", "content": "#(message)\nThanks, let's move on\n"},
    ]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    res = openai.ChatCompletion.create(
        model="gpt-4",
        messages=default_prompt()
    )
    msg = res['choices'][0]['message']['content']
    await update.message.reply_text(
        msg  # "Hi! My name is Eris. I will hold a conversation with you. "
    )


async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # archive chat by adding random base64 string end
    if update.effective_user.id in users_chats:
        users_chats[str(update.effective_user.id) + str(uuid.uuid4())] = users_chats[update.effective_user.id]
    users_chats[update.effective_user.id] = default_prompt()
    print('memory cleared')
    await update.effective_user.send_message(
        "memory cleared"
    )


async def think(messages):
    original = messages
    messages = messages.copy()

    had_error = False
    for _ in range(20):
        while num_tokens_from_messages(messages) > 5000:
            print("too big: ", num_tokens_from_messages(messages))
            messages = default_prompt() + messages[len(default_prompt()) + 1:][-50:]
        res = openai.ChatCompletion.create(
            model="gpt-4",
            messages=messages
        )
        msg = res['choices'][0]['message']['content']
        messages.append({"role": "assistant", "content": msg})
        print(f'Eris: {msg}')
        print(f"num_tokens: {num_tokens_from_messages(messages)}")
        yield msg
        if msg.startswith(MESSAGE):
            break
        if msg.count(QUERY_RESULT) > 1:
            messages.pop()
            continue
        if msg.startswith(QUERY):
            query_result = ''
            cursor = connection.cursor()
            try:
                cursor.execute(msg.replace(QUERY, '').strip())
                rows = cursor.fetchall()
                # Save the results to a CSV in-memory string buffer
                csv_buffer = io.StringIO()
                csv_writer = csv.writer(csv_buffer)

                # Write the column headers (optional)
                column_names = [desc[0] for desc in cursor.description]
                csv_writer.writerow(column_names)

                # Write the rows
                csv_writer.writerows(rows)

                # Retrieve the CSV contents as a string
                query_result = f"{QUERY_RESULT}\n```csv\n{csv_buffer.getvalue()}\n```\n"

                # Close the StringIO buffer
                if len(rows) == 0:
                    query_result = "no rows"
                csv_buffer.close()

                # Close the database connection
            except (Exception, psycopg2.DatabaseError) as error:
                connection.rollback()
                query_result = f"{QUERY_ERROR}\n```\nError: {error}\n```\n"
                messages = original.copy() + messages[-2:]
            cursor.close()
            messages.append({"role": "assistant", "content": query_result})
            print(f'Eris: {query_result}')
            yield query_result
    if not messages[-1]['content'].startswith(MESSAGE):
        yield f"{MESSAGE}\nI don't know what to say. Ask me something else.\n"

async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f'\n\nNew message from {update.effective_user.first_name}')
    print(f'{update.effective_user.first_name}: {update.message.text}')
    try:
        if update.effective_user.id not in users_chats:
            users_chats[update.effective_user.id] = default_prompt()

        messages = default_prompt() + users_chats[update.effective_user.id][len(default_prompt()):][-50:]
        messages.append({"role": "user", "content": "#(message)\n" + update.message.text})

        async for msg in think(messages):

            if msg.startswith(QUERY_RESULT):
                if len(msg) > 4000:
                    msg = msg[:4000] + '...'

            # Button markup to clear the memory
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("Clear memory", callback_data="clear")]])

            await update.message.reply_text(escape_markdown(msg, version=2), parse_mode=ParseMode.MARKDOWN_V2,
                                            reply_markup=markup if msg.startswith(MESSAGE) else None)
            if msg.startswith(QUERY):
                await update.message.reply_text("Fetching results...")

            if msg.startswith(MESSAGE):
                #messages.append({"role": "assistant", "content": msg})
                if num_tokens_from_messages(messages) > 3400:
                    await update.message.reply_text("Too many tokens. Please clear memory")
                break

        users_chats[update.effective_user.id] = messages

        if datetime.now() - on_message.t > timedelta(minutes=1):
            print('saving')
            on_message.t = datetime.now()
            with open(CHAT_FILE, 'w') as fp:
                json.dump(users_chats, fp)
            print('saving done')
    except Exception as e:
        print(e)
        await update.message.reply_text("Oppsie, something went wrong. Please try again later.")


on_message.t = datetime.now()

# Register the cleanup function to be called on exit
atexit.register(cleanup)

# Register the cleanup function to be called on SIGTERM (equivalent to SIGKILL on Unix)
signal.signal(signal.SIGTERM, lambda signum, frame: cleanup())


def main() -> None:
    """Start the bot."""
    # Create the Application and pass it your bot's token.
    application = ApplicationBuilder(). \
        token(TELEGRAM_TOKEN) \
        .get_updates_http_version('1.1').http_version('1.1').build()

    secret = os.getenv("OPENAI_API_KEY")
    openai.api_key = secret

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_message))

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("clear", clear))
    application.add_handler(CallbackQueryHandler(clear, pattern="clear"))

    application.run_polling()


if __name__ == "__main__":
    main()
