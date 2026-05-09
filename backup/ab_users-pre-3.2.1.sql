--
-- PostgreSQL database dump
--

\restrict UCH8rgcYV080ckMefC1IfH8q5TjiVGwpospplO4hqr9Z5alVaUhnwiYbV3AbaBk

-- Dumped from database version 15.17 (Debian 15.17-1.pgdg13+1)
-- Dumped by pg_dump version 15.17 (Debian 15.17-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: ab_role; Type: TABLE; Schema: public; Owner: airflow
--

CREATE TABLE public.ab_role (
    id integer NOT NULL,
    name character varying(64) NOT NULL
);


ALTER TABLE public.ab_role OWNER TO airflow;

--
-- Name: ab_user; Type: TABLE; Schema: public; Owner: airflow
--

CREATE TABLE public.ab_user (
    id integer NOT NULL,
    first_name character varying(256) NOT NULL,
    last_name character varying(256) NOT NULL,
    username character varying(512) NOT NULL,
    password character varying(256),
    active boolean,
    email character varying(512) NOT NULL,
    last_login timestamp without time zone,
    login_count integer,
    fail_login_count integer,
    created_on timestamp without time zone,
    changed_on timestamp without time zone,
    created_by_fk integer,
    changed_by_fk integer
);


ALTER TABLE public.ab_user OWNER TO airflow;

--
-- Name: ab_user_role; Type: TABLE; Schema: public; Owner: airflow
--

CREATE TABLE public.ab_user_role (
    id integer NOT NULL,
    user_id integer,
    role_id integer
);


ALTER TABLE public.ab_user_role OWNER TO airflow;

--
-- Data for Name: ab_role; Type: TABLE DATA; Schema: public; Owner: airflow
--

COPY public.ab_role (id, name) FROM stdin;
1	Admin
2	Public
3	Viewer
4	User
5	Op
\.


--
-- Data for Name: ab_user; Type: TABLE DATA; Schema: public; Owner: airflow
--

COPY public.ab_user (id, first_name, last_name, username, password, active, email, last_login, login_count, fail_login_count, created_on, changed_on, created_by_fk, changed_by_fk) FROM stdin;
3	Ops	User	operator	pbkdf2:sha256:260000$oCxoHW6lj3Qo5lhJ$3c76ef71b306b6da1fdf95adbc4f827f0dbf7b36144d631bc0c7856af2e73724	t	operator@ffengine.local	2026-04-23 21:03:24.980755	6	0	2026-04-23 16:22:40.355678	2026-04-23 16:22:40.355685	\N	\N
4	View	User	viewer	pbkdf2:sha256:260000$2axcF2xTdEJSbAQG$859d246d9cc6a871d186c79b28706507b326bee945e4ba2ec6a27eda09ce5519	t	viewer@ffengine.local	2026-04-23 21:03:26.687063	8	0	2026-04-23 16:22:40.451562	2026-04-23 16:22:40.45157	\N	\N
2	Break	Glass	breakglass	pbkdf2:sha256:260000$mVyH018LCsD2HAsm$f99ff6d6623e169ddd84ba0e6c75a0e40cb351eae395f5a63ae4407d80df1dd5	t	breakglass@ffengine.local	2026-04-23 21:03:28.252377	7	0	2026-04-23 16:22:40.271767	2026-04-23 16:22:40.271807	\N	\N
1	Admin	User	admin	pbkdf2:sha256:260000$VCBnaZFfzOw079Gh$1c955f08e04a6d0ec544946bbb5af4d28d1abf5ec98baf94cd6d65a7db7259ba	t	admin@ffengine.local	2026-04-29 11:13:22.919216	15	0	2026-04-23 16:22:40.186505	2026-04-23 16:22:40.186513	\N	\N
5	deneme	ddd	ddd	pbkdf2:sha256:260000$P6YcQCoMTFLx7d7M$e7f95a03d7ba2cb110e0c6d232d4e818cb70b12d0eb6541911f44000b09b7f3f	t	de@ff.com	\N	\N	\N	2026-04-23 17:18:29.890694	2026-04-23 17:18:29.890701	2	2
\.


--
-- Data for Name: ab_user_role; Type: TABLE DATA; Schema: public; Owner: airflow
--

COPY public.ab_user_role (id, user_id, role_id) FROM stdin;
1	1	1
2	2	1
3	3	5
4	4	3
5	5	4
\.


--
-- Name: ab_role ab_role_name_uq; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_role
    ADD CONSTRAINT ab_role_name_uq UNIQUE (name);


--
-- Name: ab_role ab_role_pkey; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_role
    ADD CONSTRAINT ab_role_pkey PRIMARY KEY (id);


--
-- Name: ab_user ab_user_email_uq; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_email_uq UNIQUE (email);


--
-- Name: ab_user ab_user_pkey; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_pkey PRIMARY KEY (id);


--
-- Name: ab_user_role ab_user_role_pkey; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_pkey PRIMARY KEY (id);


--
-- Name: ab_user_role ab_user_role_user_id_role_id_uq; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_user_id_role_id_uq UNIQUE (user_id, role_id);


--
-- Name: ab_user ab_user_username_uq; Type: CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_username_uq UNIQUE (username);


--
-- Name: idx_ab_user_username; Type: INDEX; Schema: public; Owner: airflow
--

CREATE UNIQUE INDEX idx_ab_user_username ON public.ab_user USING btree (lower((username)::text));


--
-- Name: ab_user ab_user_changed_by_fk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_changed_by_fk_fkey FOREIGN KEY (changed_by_fk) REFERENCES public.ab_user(id);


--
-- Name: ab_user ab_user_created_by_fk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user
    ADD CONSTRAINT ab_user_created_by_fk_fkey FOREIGN KEY (created_by_fk) REFERENCES public.ab_user(id);


--
-- Name: ab_user_role ab_user_role_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.ab_role(id) ON DELETE CASCADE;


--
-- Name: ab_user_role ab_user_role_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: airflow
--

ALTER TABLE ONLY public.ab_user_role
    ADD CONSTRAINT ab_user_role_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.ab_user(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict UCH8rgcYV080ckMefC1IfH8q5TjiVGwpospplO4hqr9Z5alVaUhnwiYbV3AbaBk

