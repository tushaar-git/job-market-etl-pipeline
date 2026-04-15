CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(255),
    state VARCHAR(255),
    county VARCHAR(255),
    city VARCHAR(255),
    display_name VARCHAR(255),
    latitude NUMERIC,
    longitude NUMERIC,
    UNIQUE (country, state, county, city)
);

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    label VARCHAR(255),
    tag VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) UNIQUE NOT NULL,
    title VARCHAR(255),
    description TEXT,
    salary_min NUMERIC,
    salary_max NUMERIC,
    salary_is_predicted BOOLEAN,
    created_at TIMESTAMP,
    redirect_url TEXT,
    adref TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    country VARCHAR(10),
    company_id INTEGER REFERENCES companies(company_id),
    location_id INTEGER REFERENCES locations(location_id),
    category_id INTEGER REFERENCES categories(category_id)
);
