from telethon import TelegramClient, events, errors, sync
from telethon.tl.functions.messages import GetDialogsRequest
import telethon.tl.types
import telethon.client.telegramclient
from io import TextIOWrapper
import pickle
import asyncio
import os

api_id = telega.api_id
api_hash = telega.api_hash
phone = telega.phone
report = telega.report
chats = ['SGHitch']
file_names = [i + '_seeker.pkl' for i in chats]

def find_last_message(file_name):

    try:
        with open(file_name,'rb') as file_o:
            while True:
                try:
                    last_mess = pickle.load(file_o)

                except EOFError:
                    # print(last_mess['Message']['id'])
                    return last_mess['Message']['id']

    except FileNotFoundError:
        return None

def unpacker(obj):

    if (type(obj) == list): 
        if len(obj) != 0: return [unpacker(i) for i in obj]
        else: return []
    
    try:
        dic = dict()
        for key in obj.__dict__:
            if key == '_client': continue
            
            dic[key] = unpacker(obj.__dict__[key])
        return {obj.__class__.__name__: dic}
    except:
        return obj

async def logging():

    print('########### in logging')
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        await client.sign_in(phone, input('Enter the code: '))

    print('########### connected')

async def download(chat,file_name):

    print('########### chat - ' , chat)

    last_id = find_last_message(file_name)

    await client.send_message('egor_bykov','Messages from chat {}'.format(chat))
    n = 0
    notice = await client.send_message('egor_bykov','{} messages downloaded'.format(n))

    try:
        async for mess in client.iter_messages(chat): #, offset_id = last_id, reverse = True):
            n += 1
            if n % 1000 == 0: await client.edit_message(notice,'{} messages downloaded'.format(n))
            if n % 500 == 0: await asyncio.sleep(1)
            
            print(unpacker(mess),"\n")

            # print(mess.__dict__['_action_entities'],"\n")
            # print(mess.__dict__['action'].__dict__,"\n")

            input('********** any key *************')

            with open(file_name,'ab') as file_o:
                pickler = pickle.Pickler(file_o, -1)
                pickler.dump(unpacker(mess))

    except errors.TakeoutInitDelayError as e:
        await client.send_message('Must wait ' +str(e.seconds) + ' seconds')

    f_size = os.stat(file_name).st_size / (1024*1024)

    await client.send_message('egor_bykov','Terminating... {} messages downloaded\nFile size - {:.1f}'.format(n, f_size))

async def main():

    await logging()

    await download(chats[0],file_names[0])
    # await download(chats[1],file_names[1])

    # print('########### creating tasks')



    # tasks = ( asyncio.create_task() for i in range(len(chats)) )

    # print('########### tasks created')

    # for _ in asyncio.as_completed(tasks): pass

if __name__ == "__main__":

    client = TelegramClient(phone, api_id, api_hash)
    client.loop.run_until_complete(main())

    # client.send_message('egor_bykov','Bot online!')

'''
MessageService(
        out=False,
        id=1,
        action=MessageActionChannelMigrateFrom(
                title='Entrepreneurial journey',
                chat_id=273517476
        ),
        silent=False,
        date=datetime.datetime(2017, 11, 26, 17, 40, 40, tzinfo=datetime.timezone.utc),
        legacy=False,
        post=False,
        media_unread=False,
        to_id=PeerChannel(
                channel_id=1232960120
        ),
        reply_to_msg_id=None,
        mentioned=False,
        from_id=234369112
) /n
{'fwd_from': None,
'out': False,
'_client': <telethon.client.telegramclient.TelegramClient object at 0x7f0cce902898>,
'_via_input_bot': None,
'id': 1,
'_sender_id': 234369112,
'via_bot_id': None,
'_input_sender': <telethon.tl.types.InputPeerUser object at 0x7f0ccac65390>,
'_buttons': None,
'date': datetime.datetime(2017, 11, 26, 17, 40, 40, tzinfo=datetime.timezone.utc),
'post': False,
'reply_to_msg_id': None,
'_forward': None,
'_reply_message': None,
'_input_chat': <telethon.tl.types.InputPeerChannel object at 0x7f0ccacb7f28>,
'reply_markup': None,
'mentioned': False,
'_text': None,
'_buttons_count': None,
'_sender': <telethon.tl.types.User object at 0x7f0ccac61278>,
'edit_date': None,
'restriction_reason': None,
'views': None,
'edit_hide': None,
'entities': None,
'_action_entities': [<telethon.tl.types.ChatForbidden object at 0x7f0ccacd80b8>],
'message': None,
'action': <telethon.tl.types.MessageActionChannelMigrateFrom object at 0x7f0ccacd4eb8>,
'_chat_peer': <telethon.tl.types.PeerChannel object at 0x7f0ccacd4e80>,
'silent': False,
'grouped_id': None,
'legacy': False,
'_file': None,
'to_id': <telethon.tl.types.PeerChannel object at 0x7f0ccacd4e80>,
'from_scheduled': None,
'_buttons_flat': None,
'_chat': <telethon.tl.types.Channel object at 0x7f0ccacd8198>,
'_via_bot': None,
'media': None,
'post_author': None,
'_broadcast': False,
'media_unread': False,
'from_id': 234369112}
'''