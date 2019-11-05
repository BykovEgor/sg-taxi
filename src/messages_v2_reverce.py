from telethon import TelegramClient, events, errors, sync
from telethon.tl.functions.messages import GetDialogsRequest
import telethon.tl.types
import telethon.client.telegramclient
from io import TextIOWrapper
import pickle
import asyncio
import os

api_id = 1057025
api_hash = 'd6852b9561d70f89b8464082c36d487e'
phone = '+79955062351'
report = 'bykov_egor'
chats = ['SGHitch','redmik20proglobalofficial','RedmiNote7Official'] #PocophoneGlobalOfficial 'entrepreneurialjourney'
file_names = [i + '_msg_rev.pkl' for i in chats]

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
        return 0

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

    # last_id = find_last_message(file_name)

    await client.send_message('egor_bykov','Messages from chat {}'.format(chat))
    n = 0
    notice = await client.send_message('egor_bykov','{} messages downloaded'.format(n))

    try:
        async for mess in client.iter_messages(chat, reverse = False):
            n += 1
            if n % 1000 == 0: await client.edit_message(notice,'{} messages downloaded'.format(n))
            if n % 500 == 0: await asyncio.sleep(1)
            
            # print(unpacker(mess),"\n")

            # print(mess.__dict__['_action_entities'],"\n")
            # print(mess.__dict__['action'].__dict__,"\n")

            # input('********** any key *************')

            with open(file_name,'ab') as file_o:
                pickler = pickle.Pickler(file_o, -1)
                pickler.dump(unpacker(mess))

    except errors.TakeoutInitDelayError as e:
        await client.send_message('Must wait ' +str(e.seconds) + ' seconds')

    f_size = os.stat(file_name).st_size / (1024*1024)

    await client.send_message('egor_bykov','Terminating... {} messages downloaded\nFile size - {:.1f}M'.format(n, f_size))

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