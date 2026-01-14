import logging
import asyncio
import markdown
import streamlit as st
from datetime import datetime, timezone
import os

from client.client.auth import register, login
from client.client.relations import follow_user, unfollow_user, get_followers, get_following
from client.client.posts import publish, repost, get_posts, get_posts_id, get_post

from client.client.constants import *
from client.client.discoverer import start_background_check

logger = logging.getLogger('socialnet.client.main')
# logger.setLevel(logging.INFO)

start_background_check()


async def update_cache():
    if 'logged_user' not in st.session_state:
        st.session_state.logged_user = None

    user = st.session_state.logged_user
    if not user:
        logger.info('No storage to update, no user found')
        return

    if 'token' not in st.session_state:
        logger.error('Token not found in session state')
        return

    token = st.session_state['token']

    try:
        await get_posts(user, token, request=True)
        await get_followers(user, token, request=True)

        following = await get_following(user, token, request=True)

        logger.info('Updated cache')

        for user in following:
            await get_posts(user, token, request=True)
    except Exception as e:
        logger.error(f'Error updating storage: {e}')

if 'logged_user' not in st.session_state:
    st.session_state.logged_user = None

if 'current_view' not in st.session_state:
    st.session_state.current_view = 'login'


def switch_view(view):
    st.session_state.current_view = view


def navbar():
    with st.sidebar:
        st.title('Social network')
        st.markdown('-----')
        if st.session_state.logged_user is None:
            option = st.radio('Navigate', ['Login/Register'])
        else:
            st.markdown(f'Welcome, {st.session_state.logged_user}')
            option = st.radio('Navigate', ['Relationships', 'Posts', 'Logout'])

        if option == 'Login':
            switch_view('login')
            return

        if option == 'Relationships':
            switch_view('relationships')
            return
        
        if option == 'Posts':
            switch_view('posts')

        if option == 'Logout':
            st.session_state.logged_user = None
            switch_view('login')
            st.rerun()
        
def user_stats():
    token = st.session_state['token']

    followers = asyncio.run(get_followers(st.session_state.logged_user, token))

    if followers is not None:
        cnt_followers = len(followers)
    else:
        cnt_followers = -1
        st.error('Failed to retrieve followers')

    following = asyncio.run(get_following(st.session_state.logged_user, token))
    if following is not None:
        cnt_following = len(following)
    else:
        cnt_following = -1
        st.error('Failed to retrieve following')

    st.markdown(f"""
                <div style="background-color:rgb(25, 30, 41);padding:5px 20px;margin:10px 0;border-radius:10px;">
                    <h3>📊 Stats</h3>
                    <div style="display:flex;">
                        <p><strong>Followers<strong>: {cnt_followers}</p>
                        <p style="margin-left:30px"><strong>Following<strong>: {cnt_following}</p>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,)


def handle_login(username, password):
    if '|' in username:
        st.error(f'Invalid username')
        return

    token = login(username, password)
    if token:
        st.session_state.logged_user = username
        st.session_state['token'] = token
        switch_view('relationships')
        st.rerun()
    else:
        st.error('Invalid username or password')


def handle_register(username, email, name, password):
    if '|' in username:
        st.error('Username cannot contain character |')
        return

    if len(username) < MIN_USERNAME_LENGTH or len(username) > MAX_USERNAME_LENGTH:
        st.error(f'Username length must have between {MIN_USERNAME_LENGTH} and {MAX_USERNAME_LENGTH} characters')
        return

    response = register(username, email, name, password)
    if response and response.success:
        st.success('Registration successful! Please log in')
        switch_view('login')
    else:
        st.error('Registration failed. Try again')

def login_register_view():
    st.title("🔐 Auth")
    option = st.selectbox("Choose an option", ["Login", "Register"])
    username = st.text_input("👤 Username")
    password = st.text_input("🔑 Password", type="password")

    if option == "Login":
        if st.button("Login"):
            handle_login(username, password)
    elif option == "Register":
        email = st.text_input("E-mail")
        name = st.text_input("Name")

        if st.button("Register"):
            handle_register(username, email, name, password)


def relationships_view():
    st.title(f"🤝 Relationships")
    user_stats()

    option = st.selectbox("Choose an action", [
                          "Follow a User", "View Followers", "View Following"])
    if option == "Follow a User":
        user_to_follow = st.text_input("Enter username to follow")
        token = st.session_state['token']
        if st.button("👉 Follow"):
            if '|' in user_to_follow:
                st.error('Username cannot contain |')
            else:
                response = follow_user(
                    st.session_state.logged_user, user_to_follow, token)
                if response and response.success:
                    st.success(f"You are now following {user_to_follow}.")
                    st.rerun()
                else:
                    st.error(f"Failed to follow the user. {response.message}")
    elif option == "View Followers":
        token = st.session_state['token']
        response = asyncio.run(get_followers(
            st.session_state.logged_user, token))
        if response is not None:
            st.markdown("### Your followers:")
            for follower in response:
                st.markdown(f"👥 **{follower}**")
        else:
            st.error("Failed to retrieve followers.")
    elif option == "View Following":
        token = st.session_state['token']
        response = asyncio.run(get_following(
            st.session_state.logged_user, token))
        if response is not None:
            st.markdown("### You are following:")
            for following in response:
                st.markdown(f"👥 **{following}**")
        else:
            st.error("Failed to retrieve following list.")


def format_date_time(iso_timestamp):
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        now = datetime.now(timezone.utc)

        if dt.date() == now.date():
            return f"Today · {dt.strftime('%H:%M')}"
        elif (now.date() - dt.date()).days == 1:
            return f"Yesterday · {dt.strftime('%H:%M')}"
        else:
            return dt.strftime('%b %d, %Y · %H:%M')
    except Exception as e:
        return iso_timestamp


def show_post(post):
    post_html = markdown.markdown(post.content)

    if post.is_repost:
        repost_time = format_date_time(post.timestamp)
        original_time = format_date_time(post.original_post_timestamp)
        html = f"""
        <div style="background-color:#2a2f3a; padding:16px; margin:12px 0; border-radius:12px; border-left:4px solid #888; font-family:sans-serif;">
            <div style="font-size:1.1em; color:gray; margin-bottom:8px;">
                🔁 <strong>{post.user_id}</strong>
            </div>
            <div style="background-color:#1c1f27; padding:12px; border-radius:8px;">
                <table style="width:100%; border-collapse: collapse; border: none;">
                    <tr style="border: none;">
                        <td style="border: none; padding: 0; margin: 0; color:white; font-weight:bold; font-size:1.0em;">{post.original_post_user_id}</td>
                        <td style="border: none; padding: 0; margin: 0; text-align:right; color:gray;">
                            <small>from {original_time}</small>
                        </td>
                    </tr>
                </table>
                <div style="margin-top: 12px;">{post_html}</div>
            </div>
            <div style="text-align:right; margin-top:8px;">
                <small style="color:gray;">{repost_time}</small>
            </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
    else:
        timestamp = format_date_time(post.timestamp)
        html = f"""
        <div style="background-color:#1e222b; padding:16px; margin:12px 0; border-radius:12px; font-family:sans-serif;">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <h5 style="margin:0; color:white;">{post.user_id}</h5>
                <span style="font-size:0.85em; color:gray;">{timestamp}</span>
            </div>
            <div style="margin-top: 12px;">{post_html}</div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)


def refresh_posts():
    token = st.session_state['token']
    response = asyncio.run(get_following(st.session_state.logged_user, token))
    users = [st.session_state.logged_user]

    if response is not None:
        for following in response:
            users.append(following)
    else:
        st.error('Failed to retrieve following list')

    posts = []
    for user in users:
        response_ids = asyncio.run(get_posts_id(user, token))
        if response_ids:
            for post_id in response_ids.posts_id:
                response_post = asyncio.run(get_post(post_id, token))
                if response_post:
                    posts.append(response_post.post)
                else:
                    st.error(f'Failed to load post {post_id}')
        else:
            st.error(f'Failed to load post IDs of user {user}')

    posts.sort(key=lambda post: datetime.fromisoformat(
        post.timestamp), reverse=True)

    st.session_state.posts = posts


def post_view():
    st.title('💬 Posts')
    refresh_posts()

    token = st.session_state['token']
    response_followers = asyncio.run(
        get_followers(st.session_state.logged_user, token))

    if response_followers is not None:
        followers = len(response_followers)
    else:
        followers = 0
        st.error('Failed to retrieve amount of followers')

    with st.form('post_form'):
        content = st.text_area(
            'Write a post:',
            max_chars=min(MAX_POST_LENGHT, (followers + 1) * 300),
            placeholder='What\'s on your mind?',
        )
        submitted = st.form_submit_button('Publish 🚀')
        if submitted:
            response = publish(st.session_state.logged_user, content, token)

            if response and response.success:
                refresh_posts()
                st.success(response.message)
            else:
                post = ""
                if response:
                    post = response.message
                st.error(f'Failed to publish the post: {post}')

    if st.button('🔄 Refresh posts'):
        refresh_posts()

    if 'posts' in st.session_state:
        st.markdown("#### 📧 posts:")
        for idx, post in enumerate(st.session_state.posts):
            show_post(post)

            button_key = f'repost_button_{idx}'

            if st.button('🔁 Repost', key=button_key):
                st.session_state.repost_id = post.post_id
                st.session_state.repost_clicked = True
            st.markdown('-----')

    if st.session_state.get('repost_clicked', False):
        original_message_id = st.session_state.repost_id
        repost_response = repost(
            st.session_state.logged_user, original_message_id, token)

        if repost_response and repost_response.success:
            st.success('Message reposted successfully!')
            st.session_state.repost_clicked = False
            st.rerun()
        else:
            post = ''
            if repost_response:
                post = repost_response.message
            st.error(f'Failed to repost the message: {post}')
        st.session_state.repost_clicked = False


navbar()
if st.session_state.current_view == 'login':
    login_register_view()
elif st.session_state.current_view == 'relationships':
    relationships_view()
elif st.session_state.current_view == 'posts':
    post_view()
