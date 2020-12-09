import logging

def start(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id, text="Восславим господа нашего, Омниссию!")
    logger = logging.getLogger(__name__)
    logger.info("BOT STARTED by " + update.message.from_user.first_name)


def helper(update, context):
    """Lists all available commands"""
    chat_id = update.message.chat_id
    text = "Available commands:\n" \
           "/start - starts bot\n" \
           "/status - checks if bot is still alive\n" \
           "/stop - disables bot"
    context.bot.send_message(chat_id=chat_id, text=text)


def error(update, context):
    """Log Errors caused by Updates."""
    logger = logging.getLogger(__name__)
    logger.warning('Update "%s" caused error "%s"', update, context.error)