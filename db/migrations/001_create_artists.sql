CREATE TABLE IF NOT EXISTS artists (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), 
    spotify_artist_id TEXT NOT NULL UNIQUE,
    artist TEXT NOT NULL,
    monthly_listeners INT NOT NULL,
    followers INT NOT NULL,
    popularity INT NOT NULL,
    genres TEXT[] NOT NULL,        
    images TEXT[] NOT NULL,        
    created_at TIMESTAMP DEFAULT NOW()
);