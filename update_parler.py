import  csv, time, datetime
from zipfile import ZipFile
# from io import BytesIO
import lxml.html as LH

def string_clip(whole_string, start, finish):
    place_to_start = whole_string.find(start)
    plus = len(start)
    rest = whole_string[place_to_start + plus:].strip()
    end = rest.find(finish)
    done = rest[:end].strip()
    return (done)

def now():
    '''
    Returns a timestamp as a string.
    '''
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp

def num_str(num):
    num = str(num)
    if len(num) == 1:
        num = '0' + num
    return(num)

def format_time(tuple):
    Y = tuple[0]
    MM = tuple[1]
    D = tuple[2]
    H = tuple[3]
    M = tuple[4]
    S = tuple[5]
    string_time = str(Y) + '-' + num_str(MM) + '-' + num_str(D) + ' ' + num_str(H) + ':' + num_str(M) + ':' + num_str(S)
    return(string_time)

def diff_list(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif


def find_media(postcard):
    media_wrappers = ["mc-video--wrapper", "mc-image--wrapper", 'mc-audio--wrapper']
    media_list = []
    for media_type in media_wrappers:
        for media in postcard.find_class(media_type):
            if media_type == 'mc-video--wrapper':
                media_link = media.getchildren()[0].getchildren()[0].get('src')
                media_type = media.getchildren()[0].getchildren()[0].get('type')
            if media_type == "mc-image--wrapper":
                media_link = media.getchildren()[0].get('src')
                media_type = media.getchildren()[0].get('alt')
            if media_type == 'mc-audio--wrapper':
                media_link = media.getchildren()[0].getchildren()[0].get('src')
                media_type = 'audio'
            media_link = strip_tracking(media_link)
            media_list.append({'link': media_link, 'type': media_type})
    return(media_list)

def find_website(postcard):
    media_wrappers = ['mc-website--meta--wrapper w--100 p--flex pf--col pf--jsb',
                      'mc-article--container w--100 p--flex pf--col',
                      'mc-iframe-embed--meta--wrapper w--100 p--flex pf--col pf--jsa']
    media_list = []
    for media_type in media_wrappers:
        for media in postcard.find_class(media_type):
            if media_type == 'mc-website--meta--wrapper w--100 p--flex pf--col pf--jsb':
                media_title = media.find_class("mc-website--title reblock")[0].text_content()
                media_excerpt = media.find_class("mc-website--excerpt reblock")[0].text_content()
                media_type = 'website'
                media_link = media.find_class('mc-website--link')[0].getchildren()[0].text_content().replace('\\n', '').strip()
            if media_type == 'mc-article--container w--100 p--flex pf--col':
                media_title = media.find_class("mc-article--title reblock")[0].text_content()
                media_excerpt = media.find_class("mc-article--excerpt")[0].text_content()
                media_type = 'article'
                media_link = media.find_class('mc-article--link')[0].getchildren()[0].text_content().replace('\\n', '').strip()
            if media_type == 'mc-iframe-embed--meta--wrapper w--100 p--flex pf--col pf--jsa':
                media_title = media.find_class("mc-iframe-embed--title reblock")[0].text_content()
                media_excerpt = media.find_class("mc-iframe-embed--excerpt reblock")[0].text_content()
                media_type = 'iframe'
                media_link = media.find_class('mc-iframe-embed--link')[0].getchildren()[0].get('href')
            media_link = strip_tracking(media_link)
            media_list.append({'title': media_title, 'excerpt' : media_excerpt, 'type': media_type, 'link' : media_link})
    return(media_list)

def strip_tracking(url):
    if url.find('?')>0:
        url = url[:url.find('?')]
    return(url)



zippedfile = 'parler_2020-01-06_posts-partial.zip'

post_fields = ['file', 'date_pulled', 'timestamp', 'author_name', 'author_username', 'content', 'impressions',
               'comments', 'echos', 'upvotes', 'echo statement' 'echoed_by' ,'echo_username', 'echo_timestamp',
               'echo_body', 'echo_impressions']

comment_fields = ['comment_id', 'file', 'author_name', 'author_username', 'timestamp', 'content',  'replies',
                  'echos', 'upvotes']

reply_fields = ['reply_id', 'comment_id', 'file', 'replier_name', 'replier_username', 'reply_timestamp',
                'reply_content', 'reply_replies', 'reply_echos', 'reply_upvotes', 'comment_id_file']

media_fields = ['media_id', 'source_table', 'source_id', 'echo', 'type', 'source']

website_fields = ['website_id', 'source_table', 'source_id', 'echo', 'type', 'title', 'excerpt', 'link' ]

#current_comment_number = 10000
#current_reply_number = 100000
#current_media_number = 1000000



with open('posts.csv') as csvfile:
    reader = csv.DictReader(csvfile, skipinitialspace=True)
    posts_dic = {name: [] for name in reader.fieldnames}
    for row in reader:
        for name in reader.fieldnames:
            posts_dic[name].append(row[name])

existing_files = posts_dic.get('file')
this_message = len(existing_files)

del posts_dic

out = now() + ' Restarting at file number ' + str(this_message) + '.\n'
print(out)
with open('log.txt', 'a') as writer:
    writer.write(out)

with open('comments.csv') as csvfile:
    reader = csv.DictReader(csvfile, skipinitialspace=True)
    comments_dic = {name: [] for name in reader.fieldnames}
    for row in reader:
        for name in reader.fieldnames:
            comments_dic[name].append(row[name])
current_comment_number = max(comments_dic.get('comment_id')) +1
del comments_dic

with open('replies.csv') as csvfile:
    reader = csv.DictReader(csvfile, skipinitialspace=True)
    replies_dic = {name: [] for name in reader.fieldnames}
    for row in reader:
        for name in reader.fieldnames:
            replies_dic[name].append(row[name])

current_reply_number = max(replies_dic.get('reply_id')) +1
del replies_dic

with open('media.csv') as csvfile:
    reader = csv.DictReader(csvfile, skipinitialspace=True)
    media_dic = {name: [] for name in reader.fieldnames}
    for row in reader:
        for name in reader.fieldnames:
            media_dic[name].append(row[name])

max_media = max(media_dic.get('media_id'))
del media_dic

with open('websites.csv') as csvfile:
    reader = csv.DictReader(csvfile, skipinitialspace=True)
    media_dic = {name: [] for name in reader.fieldnames}
    for row in reader:
        for name in reader.fieldnames:
            media_dic[name].append(row[name])

max_website = max(media_dic.get('website_id'))

current_media_number = max([max_website, max_media]) + 1




with ZipFile(zippedfile, mode='r') as zf:

   for message_file in zf.namelist():
       message_id = message_file[(message_file.find('/')) + 1:]
       if len(message_id) > 1 and message_id[0] != '$' and message_id not in existing_files:
       #for message_file in ['files2/a36f5e2adfd045dfa4bccb08a7c58b0c']:
           message_id = ''
           pull_time = ''
           post_timestamp = ''
           post_author = ''
           post_user = ''
           body = ''
           post_impressions = ''
           post_comments = ''
           post_echoes = ''
           post_upvotes =''
           echo_name = ''
           echo_user = ''
           echo_timestamp = ''
           echo_impressions = ''
           comment_user_name = ''
           comment_userid = ''
           comment_timestamp = ''
           comment_body = ''
           comment_reply_count = ''
           comment_echoes_count = ''
           comment_upvotes_count = ''
           echo = ''
           echo_body = ''
           post_body = ''
    
    
           total_messages = len(zf.namelist())
           with zf.open(message_file, 'r') as f:
               if this_message % 10000 == 0:
                   out = now() + ' Working on file number ' + str(this_message) + ' of ' + str(total_messages) + '.\n'
                   print(out)
                   with open ('log.txt', 'a') as writer:
                       writer.write(out)
               this_message += 1
               info = zf.getinfo(message_file)
               message_file_name = info.filename
               pull_time = format_time(info.date_time)
               message_data = str(f.read())
               message_head = string_clip(message_data, '<head>', '</head>')
               html_doc = LH.document_fromstring(message_data)
               try:
                   echo_block = html_doc.find_class('echo-byline--wrapper p--flex pf--row pf--ac')[0]
                   echo_statement = echo_block.find_class('eb--col eb--statement')[0].find_class('reblock')[0].text_content()
                   echo_timestamp = echo_block.find_class("eb--col eb--timestamp")[0].find_class('reblock')[0].text_content()
                   message_title = string_clip(message_head, 'og:title', '>')
                   echo_user = string_clip(message_title, 'content="', '-').strip()
                   echo_name  = string_clip(message_title,  '-', '-').strip()
               except:
                   echo_statement = ''
                   echo_timestamp = ''

               card_elements = []
               headers = []
               bodies = []
               footers = []

               try:
                    for comment_card in html_doc.get_element_by_id('comments-list-' + message_id).getchildren():
                       card_element = comment_card.find_class('ch--col ch--meta-col p--flex pf--col pf--jc')
                       card_elements = card_elements + card_element
                       header_elements = comment_card.find_class('card--header p--flex pf--row')
                       header =  header_elements[0]
                       headers = headers + header_elements
                       comment_user_name = header.find_class('author--name')[0].text_content()
                       comment_userid = header.find_class('author--username')[0].text_content()
                       comment_timestamp = header.find_class('post--timestamp')[0].text_content()
                       body_elements = comment_card.find_class('card--body')
                       comment_body = body_elements[0].getchildren()[0].text_content()
                       bodies = bodies + body_elements
                       footer_elements = comment_card.find_class('card--footer')
                       footer = footer_elements[0]
                       footers = footers + footer_elements
                       for foot in footer.find_class("ca--item--wrapper"):
                           for foot_elem in foot.getchildren():
                               alt = foot_elem.get('alt')
                               if alt != None:
                                   total = foot.find_class('ca--item--count')[0].text_content()
                                   if alt == 'Replies':
                                       comment_reply_count = total
                                   elif alt == 'Post Echoes':
                                       comment_echoes_count = total
                                   elif alt == 'Post Upvotes':
                                       comment_upvotes_count = total
                                   else:
                                       pass
                       comments_media = find_media(body_elements[0])
                       comments_websites = find_website(body_elements[0])
                       comment_row = [current_comment_number, message_id, comment_user_name, comment_userid,
                                      comment_timestamp,  comment_body, comment_reply_count, comment_echoes_count,
                                      comment_upvotes_count]

                       with open('comments.csv', 'a', newline='') as f:
                           write = csv.writer(f, dialect='excel')
                           write.writerow(comment_row)

                       for media in comments_media:
                           media_row = [current_media_number, 'comments', current_comment_number, '',
                                        media.get('type'), media.get('link')]
                           with open('media.csv', 'a', newline='') as f:
                               write = csv.writer(f, dialect='excel')
                               write.writerow(media_row)
                           current_media_number +=1

                       for media in comments_websites:
                           media_row = [current_media_number, 'comments', current_comment_number, '',
                                        media.get('type'), media.get('title'), media.get('excerpt'),
                                        media.get('link')]
                           with open('websites.csv', 'a', newline='') as f:
                               write = csv.writer(f, dialect='excel')
                               write.writerow(media_row)
                           current_media_number +=1

                       if int(comment_reply_count) > 0:
                           for elem in comment_card.getchildren():
                                if elem.get('class') == 'replies-list--container w--100':
                                    replylist = (elem.get('id')).replace('replies-list-', '')
                           for reply in html_doc.get_element_by_id('replies-list-' + replylist).getchildren():
                                replier = reply.find_class('author--name')[0].text_content()
                                replier_user = reply.find_class('author--username')[0].text_content()
                                reply_timestamp = reply.find_class('post--timestamp')[0].text_content()
                                reply_body = (reply.find_class('card--body')[0].text_content()).strip()
                                reply_media = find_media(reply.find_class('card--body')[0])
                                reply_websites = find_website(reply.find_class('card--body')[0])
                                reply_footer = reply.find_class('card--footer')[0]
                                for foot in reply_footer.find_class("ca--item--wrapper"):
                                    for foot_elem in foot.getchildren():
                                        alt = foot_elem.get('alt')
                                        if alt != None:
                                            total = foot.find_class('ca--item--count')[0].text_content()
                                            if alt == 'Replies':
                                                reply_reply_count = total
                                            elif alt == 'Post Echoes':
                                                reply_echoes_count = total
                                            elif alt == 'Post Upvotes':
                                                reply_upvotes_count = total
                                            else:
                                                pass
                                reply_row = [current_reply_number, current_comment_number, message_id, replier,
                                             replier_user, reply_timestamp, reply_body, reply_reply_count,
                                             reply_echoes_count, reply_upvotes_count, replylist]

                                with open('replies.csv', 'a', newline='') as f:
                                    write = csv.writer(f, dialect='excel')
                                    write.writerow(reply_row)

                                for media in reply_media:
                                    media_row = [current_media_number, 'replies', current_reply_number, '',
                                                 media.get('type'), media.get('link')]
                                    with open('media.csv', 'a', newline='') as f:
                                        write = csv.writer(f, dialect='excel')
                                        write.writerow(media_row)
                                    current_media_number += 1

                                for media in reply_websites:
                                    media_row = [current_media_number, 'replies', current_reply_number, '',
                                                 media.get('type'), media.get('title'), media.get('excerpt'),
                                                 media.get('link')]
                                    with open('websites.csv', 'a', newline='') as f:
                                        write = csv.writer(f, dialect='excel')
                                        write.writerow(media_row)
                                    current_media_number += 1

                                current_reply_number +=1

                       current_comment_number += 1


               except:
                   pass

               cards = html_doc.find_class('ch--col ch--meta-col p--flex pf--col pf--jc')
               all_headers = html_doc.find_class('card--header p--flex pf--row')
               all_bodies = html_doc.find_class('card--body')
               all_footers = html_doc.find_class('card--footer')
               non_comment_cards = diff_list(cards, card_elements)
               non_comment_headers = diff_list(all_headers, headers)
               non_comment_bodies = diff_list(all_bodies, bodies)
               non_comment_footers = diff_list(all_footers, footers)

               if len(non_comment_footers) == 1:
                   post_foot = non_comment_footers[0]
                   for foot_part in post_foot.find_class('pa--item--wrapper'):
                       for foot_elem in foot_part.getchildren():
                               alt = foot_elem.get('alt')
                               if alt != None:
                                   total = foot_part.find_class('pa--item--count')[0].text_content()
                                   if alt == 'Post Comments':
                                       post_comments = total
                                   elif alt == 'Post Echoes':
                                       post_echoes = total
                                   elif alt == 'Post Upvotes':
                                       post_upvotes = total
                                   else:
                                       pass

               non_comment_counts = 0
               while non_comment_counts < len(non_comment_cards):
                   card = non_comment_cards[non_comment_counts]
                   card_author = card.find_class('author--name')[0].text_content()
                   card_username = card.find_class('author--username')[0].text_content()
                   card_timestamp = card.find_class('post--timestamp')[0].text_content()
                   card_body = non_comment_bodies[non_comment_counts]
                   body = card_body.getchildren()[0].text_content()
                   media_files = find_media(card_body)
                   article_files = find_website(card_body)
                   try:
                     card_impressions = card.find_class('impressions--count')[0].text_content()
                   except:
                       card_impressions = ''
                   if card_username == echo_user:
                       echo_name = card_author
                       echo_user = card_username
                       echo_timestamp = card_timestamp
                       echo_impressions = card_impressions
                       echo = 'True'
                       echo_body = body
                   else:
                       post_author = card_author
                       post_user = card_username
                       post_timestamp = card_timestamp
                       post_impressions = card_impressions
                       post_body = body
                       echos = 'False'

                   for media in media_files:
                       media_row = [current_media_number, 'posts', message_id, echo,
                                    media.get('type'), media.get('link')]
                       with open('media.csv', 'a', newline='') as f:
                           write = csv.writer(f, dialect='excel')
                           write.writerow(media_row)
                       current_media_number += 1

                   for media in article_files:
                       media_row = [current_media_number, 'posts', current_comment_number, echo,
                                    media.get('type'), media.get('title'), media.get('excerpt'),
                                    media.get('link')]
                       with open('websites.csv', 'a', newline='') as f:
                           write = csv.writer(f, dialect='excel')
                           write.writerow(media_row)
                       current_media_number +=1


                   non_comment_counts +=1

               post_row = [message_id, pull_time, post_timestamp, post_author, post_user, post_body, post_impressions,
                           post_comments, post_echoes, post_upvotes, echo_name, echo_user, echo_timestamp, echo_body,
                           echo_impressions]
               with open('posts.csv', 'a', newline='') as f:
                   write = csv.writer(f, dialect='excel', )
                   write.writerow(post_row)
