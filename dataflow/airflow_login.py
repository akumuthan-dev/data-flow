# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#  Modified by the Department for International Trade
"""
Override this file to handle your authenticating / login.

Copy and alter this file and put in your PYTHONPATH as airflow_login.py,
the new module will override this one.

"""
import flask_login
from flask_login import login_required, current_user, logout_user, login_user  # noqa: F401
from flask_oauthlib.client import OAuth

from flask import url_for, redirect, request

from werkzeug import security

from airflow import settings  # noqa: F401
from airflow import models
from airflow.utils.db import provide_session
from airflow.utils.log.logging_mixin import LoggingMixin

from dataflow import constants


log = LoggingMixin().log


class StaffUser(models.User):

    def __init__(self, user):
        self.user = user

    @property
    def is_active(self):
        """Required by flask_login"""
        return True

    @property
    def is_authenticated(self):
        """Required by flask_login"""
        return True

    @property
    def is_anonymous(self):
        """Required by flask_login"""
        return False

    def get_id(self):
        """Returns the current user id as required by flask_login"""
        return self.user.get_id()

    def data_profiling(self):
        """Provides access to data profiling tools"""
        return True

    def is_superuser(self):
        """Access all the things"""
        return True


class AuthenticationError(Exception):
    pass


class AuthbrokerBackend(object):

    def __init__(self):
        self.login_manager = flask_login.LoginManager()
        self.login_manager.login_view = 'airflow.login'
        self.flask_app = None
        self.auth_path = 'o/authorize/'
        self.token_path = 'o/token/'
        self.me_path = 'api/v1/user/me/'
        self.client_id = constants.AUTHBROKER_CLIENT_ID
        self.client_secret = constants.AUTHBROKER_CLIENT_SECRET
        self.allowed_domains = constants.AUTHBROKER_ALLOWED_DOMAINS.split(',')
        self.base_url = constants.AUTHBROKER_URL

    def init_app(self, flask_app):
        self.flask_app = flask_app

        self.login_manager.init_app(self.flask_app)

        self.authbroker_client = OAuth(self.flask_app).remote_app(
            'authbroker',
            base_url=self.base_url,
            request_token_url=None,
            access_token_url=f'{self.base_url}{self.token_path}',
            authorize_url=f'{self.base_url}{self.auth_path}',
            consumer_key=self.client_id,
            consumer_secret=self.client_secret,
            access_token_method='POST',
            request_token_params={
                'state': lambda: security.gen_salt(10)
            }
        )

        self.login_manager.user_loader(self.load_user)

        self.flask_app.add_url_rule('/oauth2callback',
                                    'oauth2callback',
                                    self.oauth2callback)

    def login(self, request):
        log.info('================Redirecting===================')
        return self.authbroker_client.authorize(callback=url_for(
            'oauth2callback',
            _external=True),
            state=request.args.get('next') or request.referrer or None)

    def get_user_profile_email(self, authbroker_token):
        log.info('================Getting user porfile===================')
        resp = self.authbroker_client.get(
            f'{self.base_url}{self.me_path}',
            token=(authbroker_token, '')
        )

        if not resp or resp.status != 200:
            log.info('user profile repsponse failed')
            raise AuthenticationError(
                'Failed to fetch user profile, status ({0})'.format(
                    resp.status if resp else 'None'))
        log.info('user profile resp ========= {}'.format(resp.__dict__))

        return resp.data['email']

    def domain_check(self, email):
        domain = email.split('@')[1]
        log.info('======DOMAIN {} ==========='.format(domain))
        log.info('======ALLOWED DOMAIN {} ========'.format(self.allowed_domains))
        if domain in self.allowed_domains:
            return True
        return False

    @provide_session
    def load_user(self, userid, session=None):
        if not userid or userid == 'None':
            return None

        user = session.query(models.User).filter(
            models.User.id == int(userid)).first()
        return StaffUser(user)

    @provide_session
    def oauth2callback(self, session=None):
        log.debug('Authbroker callback called')

        next_url = request.args.get('state') or url_for('admin.index')
        resp = self.authbroker_client.authorized_response()
        try:
            if resp is None or resp.get('access_token') is None:
                log.info('===========Couldnt fetch access token=============')
                raise AuthenticationError(
                    'Access denied: reason={0} error={1} resp={2}'.format(
                        request.args['error'],
                        request.args['error_description'],
                        resp
                    )
                )

            authbroker_token = resp['access_token']
            email = self.get_user_profile_email(authbroker_token)

            if not self.domain_check(email):
                log.info('==========Domain check failed ==================')
                return redirect(url_for('airflow.noaccess'))

        except AuthenticationError:
            log.info('================Redirecting===================')
            return redirect(url_for('airflow.noaccess'))

        user = session.query(models.User).filter(
            models.User.username == email).first()

        if not user:
            user = models.User(
                username=email,
                email=email,
                is_superuser=False
            )

        session.merge(user)
        session.commit()
        login_user(StaffUser(user))
        session.commit()

        return redirect(next_url)


login_manager = AuthbrokerBackend()


def login(self, request):
    return login_manager.login(request)
