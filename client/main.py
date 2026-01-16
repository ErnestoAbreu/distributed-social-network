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
from client.client.discoverer import start_background_check, NoServersAvailableError, NO_SERVERS_AVAILABLE_MESSAGE


logger = logging.getLogger('socialnet.client.main')
# logger.setLevel(logging.INFO)

# Configuración de la página
st.set_page_config(
    page_title="Social Network",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

start_background_check()


def _handle_no_servers(err=None) -> None:
    _popup_error(NO_SERVERS_AVAILABLE_MESSAGE)
    if err is not None:
        logger.warning('No servers available: %s', err)


def _clear_session_keys(*keys: str) -> None:
    for key in keys:
        st.session_state.pop(key, None)


def _enter_view(view_name: str) -> None:
    """Runs once when entering a view to avoid stale UI state across pages."""
    previous = st.session_state.get('_active_view')
    if previous == view_name:
        return

    st.session_state['_active_view'] = view_name

    # Clear UI-only state that should not leak between views.
    if view_name == 'login':
        _clear_session_keys('posts', 'repost_clicked', 'repost_id', 'follow_username')
    elif view_name == 'relationships':
        _clear_session_keys('follow_username')
    elif view_name == 'posts':
        _clear_session_keys('repost_clicked', 'repost_id')
        # Force a fresh feed load on entry.
        st.session_state['posts'] = None


def _popup_error(message: str) -> None:
    """Show an error as a popup-style notification (native Streamlit).

    Uses st.toast when available; falls back to st.error.
    """
    try:
        st.toast(message, icon="❌")
    except Exception:
        st.error(message)


def _popup_warning(message: str) -> None:
    """Show a warning as a popup-style notification (native Streamlit).

    Uses st.toast when available; falls back to st.warning.
    """
    try:
        st.toast(message, icon="⚠️")
    except Exception:
        st.warning(message)


def _popup_info(message: str) -> None:
    """Show an info message"""
    st.info(message)


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

if 'token' not in st.session_state:
    st.session_state['token'] = None


def switch_view(view):
    st.session_state.current_view = view


def navbar():
    with st.sidebar:
        st.markdown("# 🌐 Social Network")
        st.markdown("---")
        
        if st.session_state.logged_user is None:
            _popup_info("👋 **Welcome!**\n\nPlease login or register to get started.")
            option = st.radio('Authentication', ['Login/Register'], label_visibility="collapsed")
        else:
            st.success(f"**Logged in as:**\n### @{st.session_state.logged_user}")
            st.markdown("---")
            
            option = st.radio(
                '**Navigation**',
                ['💬 Posts', '🤝 Relationships', '🚪 Logout'],
                format_func=lambda x: x.split(' ', 1)[1]
            )

        if option == 'Login/Register':
            switch_view('login')
            return

        if option == '🤝 Relationships' or option == 'Relationships':
            switch_view('relationships')
            return
        
        if option == '💬 Posts' or option == 'Posts':
            switch_view('posts')

        if option == '🚪 Logout' or option == 'Logout':
            st.session_state.logged_user = None
            _clear_session_keys('token', 'posts', 'repost_clicked', 'repost_id', 'follow_username')
            switch_view('login')
            st.rerun()
        
def user_stats():
    token = st.session_state.get('token')
    if not st.session_state.get('logged_user') or not token:
        _popup_info('🔐 **Please log in to see your stats.**')
        return

    try:
        followers = asyncio.run(get_followers(st.session_state.logged_user, token))
    except NoServersAvailableError as e:
        _handle_no_servers(e)
        return

    if followers is not None:
        cnt_followers = len(followers)
    else:
        cnt_followers = 0
        _popup_error('⚠️ **Failed to retrieve followers**')

    try:
        following = asyncio.run(get_following(st.session_state.logged_user, token))
    except NoServersAvailableError as e:
        _handle_no_servers(e)
        return
    if following is not None:
        cnt_following = len(following)
    else:
        cnt_following = 0
        _popup_error('⚠️ **Failed to retrieve following list**')

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        st.metric(
            label="👥 Followers", 
            value=cnt_followers,
            help="Users following you"
        )
    with col2:
        st.metric(
            label="➕ Following", 
            value=cnt_following,
            help="Users you follow"
        )


def handle_login(username, password):
    if not username or not password:
        _popup_error('❌ **Please fill in all fields**')
        return
        
    if '|' in username:
        _popup_error('❌ **Invalid username format**')
        return

    try:
        with st.spinner('🔄 Logging in...'):
            token = login(username, password)
    except NoServersAvailableError as e:
        _handle_no_servers(e)
        return
        
    if token:
        st.session_state.logged_user = username
        st.session_state['token'] = token
        st.success(f'✅ **Welcome back, {username}!**')
        switch_view('posts')
        st.rerun()
    else:
        _popup_error('❌ **Invalid credentials** - Please check your username and password')


def handle_register(username, email, name, password):
    if not username or not email or not name or not password:
        _popup_error('❌ **Please fill in all fields**')
        return
        
    if '|' in username:
        _popup_error('❌ **Username cannot contain the "|" character**')
        return

    if len(username) < MIN_USERNAME_LENGTH or len(username) > MAX_USERNAME_LENGTH:
        _popup_error(f'❌ **Username must be between {MIN_USERNAME_LENGTH} and {MAX_USERNAME_LENGTH} characters**')
        return

    try:
        with st.spinner('🔄 Creating your account...'):
            response = register(username, email, name, password)
    except NoServersAvailableError as e:
        _handle_no_servers(e)
        return
        
    if response and response.success:
        st.success('✅ **Registration successful!** You can now log in.')
        st.balloons()
        switch_view('login')
    else:
        _popup_error('❌ **Registration failed** - Username or email might already exist')

def login_register_view():
    _enter_view('login')
    # Centrar el contenido
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("# 🔐 Welcome")
        st.markdown("### Connect with your network")
        st.markdown("---")
        
        tab1, tab2 = st.tabs(["🔑 **Login**", "📝 **Register**"])
        
        with tab1:
            st.markdown("#### Sign in to your account")
            st.markdown("")
            
            with st.form("login_form", clear_on_submit=False):
                username = st.text_input(
                    "Username",
                    placeholder="Enter your username",
                    help="Your unique username"
                )
                password = st.text_input(
                    "Password",
                    type="password",
                    placeholder="Enter your password",
                    help="Your account password"
                )
                
                st.markdown("")
                col_btn1, col_btn2 = st.columns([1, 1])
                with col_btn1:
                    submitted = st.form_submit_button(
                        "🚀 Login", 
                        use_container_width=True, 
                        type="primary"
                    )
                
                if submitted:
                    handle_login(username, password)
        
        with tab2:
            st.markdown("#### Create your account")
            st.markdown("")
            
            with st.form("register_form", clear_on_submit=False):
                username = st.text_input(
                    "Username",
                    placeholder="Choose a unique username",
                    help=f"Between {MIN_USERNAME_LENGTH} and {MAX_USERNAME_LENGTH} characters"
                )
                email = st.text_input(
                    "Email",
                    placeholder="your.email@example.com",
                    help="Your email address"
                )
                name = st.text_input(
                    "Full Name",
                    placeholder="Your full name",
                    help="Your display name"
                )
                password = st.text_input(
                    "Password",
                    type="password",
                    placeholder="Choose a secure password",
                    help="Create a strong password"
                )
                
                st.markdown("")
                col_btn1, col_btn2 = st.columns([1, 1])
                with col_btn1:
                    submitted = st.form_submit_button(
                        "✨ Create Account", 
                        use_container_width=True, 
                        type="primary"
                    )
                
                if submitted:
                    handle_register(username, email, name, password)


def relationships_view():
    _enter_view('relationships')
    token = st.session_state.get('token')
    if not st.session_state.get('logged_user') or not token:
        _popup_warning('🔐 **Your session expired. Please log in again.**')
        switch_view('login')
        st.rerun()

    st.markdown("# 🤝 Relationships")
    st.markdown("### Manage your connections")
    st.markdown("---")
    
    # Estadísticas
    user_stats()
    st.markdown("---")
    
    # Tabs para las acciones
    tab1, tab2, tab3 = st.tabs([
        "➕ **Follow Users**", 
        "👥 **Your Followers**", 
        "📋 **Following List**"
    ])
    
    with tab1:
        st.markdown("#### Discover and follow new users")
        st.markdown("")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            user_to_follow = st.text_input(
                "Username to follow",
                placeholder="@username",
                label_visibility="collapsed",
                help="Enter the username you want to follow",
                key='follow_username'
            )
        with col2:
            follow_btn = st.button(
                "➕ Follow", 
                use_container_width=True, 
                type="primary",
                help="Follow this user"
            )
        
        if follow_btn:
            token = st.session_state.get('token')
            if not token:
                _popup_warning('🔐 **Your session expired. Please log in again.**')
                switch_view('login')
                st.rerun()
            if not user_to_follow:
                _popup_warning('⚠️ **Please enter a username**')
            elif '|' in user_to_follow:
                _popup_error('❌ **Invalid username format**')
            else:
                try:
                    with st.spinner(f'🔄 Following @{user_to_follow}...'):
                        response = asyncio.run(follow_user(
                            st.session_state.logged_user, user_to_follow, token))
                except NoServersAvailableError as e:
                    _handle_no_servers(e)
                    response = None
                
                if response and response.success:
                    st.success(f"✅ **You're now following @{user_to_follow}!**")
                    st.balloons()
                    st.rerun()
                else:
                    error_msg = response.message if response else 'Unknown error'
                    _popup_error(f"❌ **Could not follow user** - {error_msg}")
    
    with tab2:
        st.markdown("#### People following you")
        st.markdown("")
        
        token = st.session_state.get('token')
        if not token:
            _popup_warning('🔐 **Your session expired. Please log in again.**')
            switch_view('login')
            st.rerun()
        
        following_set = set()
        with st.spinner('🔄 Loading followers...'):
            try:
                response = asyncio.run(get_followers(st.session_state.logged_user, token))
                following = asyncio.run(get_following(st.session_state.logged_user, token))
                if following:
                    following_set = set(following)
            except NoServersAvailableError as e:
                _handle_no_servers(e)
                response = None
        
        if response is not None:
            if len(response) == 0:
                _popup_info("📭 **No followers yet**\n\nShare your profile to gain followers!")
            else:
                st.success(f"**{len(response)} follower{'s' if len(response) != 1 else ''}**")
                st.markdown("")
                
                for idx, follower in enumerate(response):
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"#### 👤 @{follower}")
                        with col2:
                            already_following = follower in following_set
                            follow_back = st.button(
                                "✅ Following" if already_following else "➕ Follow back",
                                use_container_width=True,
                                key=f"follow_back_{idx}_{follower}",
                                help=(
                                    "You are already following this user"
                                    if already_following
                                    else "Follow this user back"
                                ),
                                disabled=already_following,
                            )

                        if follow_back and not already_following:
                            try:
                                with st.spinner(f"🔄 Following @{follower}..."):
                                    follow_response = asyncio.run(follow_user(
                                        st.session_state.logged_user,
                                        follower,
                                        token,
                                    ))
                            except NoServersAvailableError as e:
                                _handle_no_servers(e)
                                follow_response = None

                            if follow_response and follow_response.success:
                                st.success(f"✅ **You're now following @{follower}!**")
                                st.balloons()
                                st.rerun()
                            else:
                                error_msg = (
                                    follow_response.message
                                    if follow_response
                                    else 'Unknown error'
                                )
                                _popup_error(f"❌ **Could not follow user** - {error_msg}")
                        if idx < len(response) - 1:
                            st.divider()
        else:
            _popup_error("❌ **Failed to load followers** - Please try again")
    
    with tab3:
        st.markdown("#### Users you're following")
        st.markdown("")
        
        token = st.session_state.get('token')
        if not token:
            _popup_warning('🔐 **Your session expired. Please log in again.**')
            switch_view('login')
            st.rerun()
        
        with st.spinner('🔄 Loading following list...'):
            try:
                response = asyncio.run(get_following(st.session_state.logged_user, token))
            except NoServersAvailableError as e:
                _handle_no_servers(e)
                response = None
        
        if response is not None:
            if len(response) == 0:
                _popup_info("📭 **Not following anyone yet**\n\nStart following users to see their posts in your feed!")
            else:
                st.success(f"**Following {len(response)} user{'s' if len(response) != 1 else ''}**")
                st.markdown("")
                
                for idx, following in enumerate(response):
                    with st.container():
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            st.markdown(f"#### 👤 @{following}")
                        with col2:
                            unfollow_btn = st.button(
                                "➖ Unfollow",
                                use_container_width=True,
                                key=f"unfollow_{idx}_{following}",
                                help="Unfollow this user",
                                type="secondary"
                            )
                        
                        if unfollow_btn:
                            try:
                                with st.spinner(f"🔄 Unfollowing @{following}..."):
                                    unfollow_response = asyncio.run(unfollow_user(
                                        st.session_state.logged_user,
                                        following,
                                        token,
                                    ))
                            except NoServersAvailableError as e:
                                _handle_no_servers(e)
                                unfollow_response = None

                            if unfollow_response and unfollow_response.success:
                                st.success(f"✅ **You unfollowed @{following}!**")
                                st.rerun()
                            else:
                                error_msg = (
                                    unfollow_response.message
                                    if unfollow_response
                                    else 'Unknown error'
                                )
                                _popup_error(f"❌ **Could not unfollow user** - {error_msg}")
                        
                        if idx < len(response) - 1:
                            st.divider()
        else:
            _popup_error("❌ **Failed to load following list** - Please try again")


def format_date_time(iso_timestamp):
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        now = datetime.now(timezone.utc)

        if dt.date() == now.date():
            return f"Today at {dt.strftime('%H:%M')}"
        elif (now.date() - dt.date()).days == 1:
            return f"Yesterday at {dt.strftime('%H:%M')}"
        else:
            return dt.strftime('%b %d, %Y at %H:%M')
    except Exception as e:
        return iso_timestamp


def show_post(post, idx):
    with st.container():
        if post.is_repost:
            st.caption(f"🔁 **@{post.user_id}** reposted · {format_date_time(post.timestamp)}")
            
            with st.expander(f"**@{post.original_post_user_id}** · {format_date_time(post.original_post_timestamp)}", expanded=True):
                post_content = markdown.markdown(post.content)
                st.markdown(post_content, unsafe_allow_html=True)
        else:
            col1, col2 = st.columns([2, 1])
            with col1:
                st.markdown(f"### 👤 @{post.user_id}")
            with col2:
                st.caption(f"🕒 {format_date_time(post.timestamp)}")
            
            st.markdown("")
            post_content = markdown.markdown(post.content)
            st.markdown(post_content, unsafe_allow_html=True)
        
        st.markdown("")
        
        # Botones de acción
        col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
        with col1:
            if st.button(
                '🔁 Repost', 
                key=f'repost_button_{idx}', 
                use_container_width=True,
                help="Share this post with your followers"
            ):
                st.session_state.repost_id = post.post_id
                st.session_state.repost_clicked = True
                st.rerun()
        
        st.divider()


def refresh_posts():
    token = st.session_state.get('token')
    if not st.session_state.get('logged_user') or not token:
        _popup_warning('🔐 **Your session expired. Please log in again.**')
        switch_view('login')
        st.rerun()
    
    with st.spinner('🔄 Loading posts from your network...'):
        try:
            response = asyncio.run(get_following(st.session_state.logged_user, token))
        except NoServersAvailableError as e:
            _handle_no_servers(e)
            st.session_state.posts = []
            return
        users = [st.session_state.logged_user]

        if response is not None:
            for following in response:
                users.append(following)
        else:
            _popup_error('❌ **Failed to retrieve following list**')

        posts = []
        for user in users:
            try:
                response_ids = asyncio.run(get_posts_id(user, token))
            except NoServersAvailableError as e:
                _handle_no_servers(e)
                st.session_state.posts = []
                return
            if response_ids:
                for post_id in response_ids.posts_id:
                    try:
                        response_post = asyncio.run(get_post(post_id, token))
                    except NoServersAvailableError as e:
                        _handle_no_servers(e)
                        st.session_state.posts = []
                        return
                    if response_post:
                        posts.append(response_post.post)
                    else:
                        _popup_warning(f'⚠️ Could not load post {post_id}')
            # No mostrar error si un usuario no tiene posts

        posts.sort(key=lambda post: datetime.fromisoformat(
            post.timestamp), reverse=True)

        st.session_state.posts = posts

def post_view():
    _enter_view('posts')

    st.markdown('# 💬 Posts')
    st.markdown('### Share your thoughts with the network')
    st.markdown("---")

    token = st.session_state.get('token')
    if not st.session_state.get('logged_user') or not token:
        _popup_warning('🔐 **Your session expired. Please log in again.**')
        switch_view('login')
        st.rerun()

    try:
        response_followers = asyncio.run(
            get_followers(st.session_state.logged_user, token))
    except NoServersAvailableError as e:
        _handle_no_servers(e)
        return

    if response_followers is not None:
        followers = len(response_followers)
    else:
        followers = 0
        _popup_error('⚠️ **Could not retrieve follower count**')

    # Formulario para crear post
    with st.expander("✍️ **Create a new post**", expanded=True):
        max_length = min(MAX_POST_LENGHT, (followers + 1) * 300)
        
        with st.form('post_form', clear_on_submit=True):
            content = st.text_area(
                'Your post',
                max_chars=max_length,
                placeholder="What's on your mind? Share your thoughts...",
                height=120,
                help=f"Maximum {max_length} characters (based on your {followers} follower{'s' if followers != 1 else ''})"
            )
            
            col_info, col_btn = st.columns([3, 1])
            with col_info:
                pass
            with col_btn:
                submitted = st.form_submit_button(
                    '🚀 Publish', 
                    use_container_width=True, 
                    type="primary"
                )
            
            if submitted:
                if not content or not content.strip():
                    _popup_warning('⚠️ **Post cannot be empty** - Write something first!')
                else:
                    with st.spinner('📤 Publishing your post...'):
                        try:
                            response = publish(st.session_state.logged_user, content, token)
                        except NoServersAvailableError as e:
                            _handle_no_servers(e)
                            response = None

                    if response and response.success:
                        st.success(f'✅ **{response.message}**')
                        st.balloons()
                        st.session_state.posts = None
                        st.rerun()
                    else:
                        error_msg = response.message if response else "Unknown error"
                        _popup_error(f'❌ **Failed to publish** - {error_msg}')

    st.markdown("---")
    
    # Botón para refrescar
    col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
    with col1:
        if st.button('🔄 Refresh Feed', use_container_width=True, help="Load latest posts"):
            refresh_posts()
            st.rerun()

    st.markdown("")

    # Cargar y mostrar posts
    if 'posts' not in st.session_state or st.session_state.posts is None:
        refresh_posts()
    
    if 'posts' in st.session_state and st.session_state.posts:
        post_count = len(st.session_state.posts)
        st.markdown(f"### 📰 Your Feed")
        st.caption(f"**{post_count} post{'s' if post_count != 1 else ''} from your network**")
        st.markdown("---")
        
        for idx, post in enumerate(st.session_state.posts):
            show_post(post, idx)
    else:
        _popup_info("📭 **Your feed is empty**\n\nFollow users to see their posts here, or create your first post!")

    # Manejar repost
    if st.session_state.get('repost_clicked', False):
        original_message_id = st.session_state.repost_id
        
        with st.spinner('🔄 Reposting...'):
            try:
                repost_response = repost(
                    st.session_state.logged_user, original_message_id, token)
            except NoServersAvailableError as e:
                _handle_no_servers(e)
                repost_response = None

        if repost_response and repost_response.success:
            st.success('✅ **Post reposted successfully!**')
            st.balloons()
            st.session_state.posts = None
            st.session_state.repost_clicked = False
            st.rerun()
        else:
            error_msg = repost_response.message if repost_response else 'Unknown error'
            _popup_error(f'❌ **Failed to repost** - {error_msg}')
        
        st.session_state.repost_clicked = False


# Navegación principal
navbar()

# Hard guard: never render protected views without a valid session.
if st.session_state.current_view != 'login':
    if not st.session_state.get('logged_user') or not st.session_state.get('token'):
        st.session_state.current_view = 'login'

if st.session_state.current_view == 'login':
    login_register_view()
elif st.session_state.current_view == 'relationships':
    relationships_view()
elif st.session_state.current_view == 'posts':
    post_view()