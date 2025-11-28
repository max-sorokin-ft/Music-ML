CREATE TABLE IF NOT EXISTS albums (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),    
    spotify_album_id TEXT NOT NULL UNIQUE,                  
    album TEXT NOT NULL,                                     
    artists TEXT[] NOT NULL,                                
    artist_ids TEXT[] NOT NULL,                             
    album_type TEXT NOT NULL,                               
    release_date DATE NOT NULL,
    total_tracks INT NOT NULL,                            
    images TEXT[] NOT NULL,                                          
    created_at TIMESTAMP DEFAULT NOW()
);