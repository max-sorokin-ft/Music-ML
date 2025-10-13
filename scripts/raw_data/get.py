import json
import logging
from google.cloud import storage
from tqdm import tqdm
import argparse

from scripts.utils.gcs_utils import get_artists_from_gcs, get_artist_grouped_songs_from_gcs, get_artist_songs_from_gcs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

def create_multi_artist_review_html(all_artists_data, zero_streams_songs):
    """Create an HTML report with all artists and their grouped songs"""
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Multi-Artist Song Grouping Review (First 20 Artists)</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f8f9fa; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            .artist-section {{ 
                margin-bottom: 30px; 
                border: 1px solid #ddd; 
                border-radius: 8px; 
                background: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .artist-header {{ 
                background: #1db954; 
                color: white; 
                padding: 15px 20px; 
                cursor: pointer; 
                font-size: 18px; 
                font-weight: bold;
                border-radius: 8px 8px 0 0;
            }}
            .artist-header:hover {{ background: #1ed760; }}
            .artist-content {{ 
                display: none; 
                padding: 20px; 
            }}
            .song-group {{ 
                margin-bottom: 25px; 
                border: 1px solid #e0e0e0; 
                border-radius: 6px; 
                padding: 15px; 
                background: #fafafa;
            }}
            .song-name {{ 
                font-size: 16px; 
                font-weight: bold; 
                color: #1db954; 
                margin-bottom: 12px; 
                border-bottom: 1px solid #e0e0e0;
                padding-bottom: 8px;
            }}
            .variant {{ 
                margin: 8px 0; 
                padding: 12px; 
                background: white; 
                border-radius: 4px;
                border-left: 4px solid #1db954;
                display: flex;
                align-items: flex-start;
                gap: 10px;
            }}
            .variant-checkbox {{
                margin-top: 2px;
            }}
            .variant-content {{
                flex: 1;
            }}
            .variant a {{ 
                color: #1db954; 
                text-decoration: none; 
                font-weight: bold; 
                font-size: 14px;
            }}
            .variant a:hover {{ text-decoration: underline; }}
            .album {{ color: #666; font-style: italic; font-size: 12px; margin-top: 4px; }}
            .duration {{ color: #888; font-size: 11px; }}
            .streams {{ color: #1db954; font-weight: bold; font-size: 12px; }}
            .zero-streams {{ color: #dc3545; font-weight: bold; font-size: 12px; }}
            .spotify-id {{ color: #6c757d; font-size: 10px; font-family: monospace; margin-top: 2px; }}
            .summary {{ 
                background: #e8f5e8; 
                padding: 15px; 
                border-radius: 6px; 
                margin-bottom: 20px;
                text-align: center;
            }}
            .zero-streams-section {{
                background: #fff3cd;
                border: 2px solid #ffc107;
                border-radius: 8px;
                margin-top: 30px;
                padding: 20px;
            }}
            .zero-streams-header {{
                background: #ffc107;
                color: #212529;
                padding: 15px 20px;
                margin: -20px -20px 20px -20px;
                font-size: 18px;
                font-weight: bold;
                border-radius: 6px 6px 0 0;
            }}
            .zero-streams-song {{
                margin: 8px 0;
                padding: 10px;
                background: white;
                border-radius: 4px;
                border-left: 4px solid #ffc107;
                display: flex;
                align-items: flex-start;
                gap: 10px;
            }}
            .zero-streams-content {{
                flex: 1;
            }}
            .export-controls {{
                background: #e3f2fd;
                border: 1px solid #2196f3;
                border-radius: 6px;
                padding: 15px;
                margin-bottom: 20px;
                text-align: center;
            }}
            .export-button {{
                background: #4caf50;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                margin: 0 5px;
            }}
            .export-button:hover {{
                background: #45a049;
            }}
            .select-all-button {{
                background: #ff9800;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 12px;
                margin: 0 5px;
            }}
            .select-all-button:hover {{
                background: #f57c00;
            }}
            .toggle-all {{ 
                background: #007bff; 
                color: white; 
                border: none; 
                padding: 10px 20px; 
                border-radius: 4px; 
                cursor: pointer; 
                margin-bottom: 20px;
            }}
        </style>
        <script>
            // Local storage functions for checkmarks
            function saveCheckboxState() {{
                const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                const checkboxStates = {{}};
                checkboxes.forEach(checkbox => {{
                    checkboxStates[checkbox.dataset.songId] = checkbox.checked;
                }});
                localStorage.setItem('songCheckboxStates', JSON.stringify(checkboxStates));
            }}
            
            function loadCheckboxState() {{
                const savedStates = localStorage.getItem('songCheckboxStates');
                if (savedStates) {{
                    const checkboxStates = JSON.parse(savedStates);
                    const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                    checkboxes.forEach(checkbox => {{
                        if (checkboxStates.hasOwnProperty(checkbox.dataset.songId)) {{
                            checkbox.checked = checkboxStates[checkbox.dataset.songId];
                        }}
                    }});
                }}
            }}
            
            function toggleArtist(artistName) {{
                const content = document.getElementById(artistName);
                content.style.display = content.style.display === 'none' ? 'block' : 'none';
            }}
            
            function toggleAll() {{
                const contents = document.querySelectorAll('.artist-content');
                const isHidden = contents[0].style.display === 'none';
                contents.forEach(content => {{
                    content.style.display = isHidden ? 'block' : 'none';
                }});
            }}
            
            function selectAll() {{
                const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                checkboxes.forEach(checkbox => {{
                    checkbox.checked = true;
                }});
                saveCheckboxState();
            }}
            
            function selectNone() {{
                const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                checkboxes.forEach(checkbox => {{
                    checkbox.checked = false;
                }});
                saveCheckboxState();
            }}
            
            function exportToCSV() {{
                const checkboxes = document.querySelectorAll('input[type="checkbox"]:checked');
                if (checkboxes.length === 0) {{
                    alert('Please select at least one song to export.');
                    return;
                }}
                
                let csvContent = "Song Name,Artist,Album,Duration (mm:ss),Streams,Spotify ID,Spotify URL\\n";
                
                checkboxes.forEach(checkbox => {{
                    const variant = checkbox.closest('.variant, .zero-streams-song');
                    const songName = variant.querySelector('a').textContent;
                    const album = variant.querySelector('.album').textContent;
                    const duration = variant.querySelector('.duration').textContent.replace('Duration: ', '');
                    const streams = variant.querySelector('.streams, .zero-streams').textContent.replace('Streams: ', '').replace(',', '');
                    const spotifyId = variant.querySelector('.spotify-id').textContent.replace('ID: ', '');
                    const spotifyUrl = variant.querySelector('a').href;
                    
                    // Extract artist from album text (format: "Album - Artist")
                    const albumParts = album.split(' - ');
                    const albumName = albumParts[0];
                    const artist = albumParts[1] || 'Unknown';
                    
                    csvContent += `"${{songName}}","${{artist}}","${{albumName}}","${{duration}}","${{streams}}","${{spotifyId}}","${{spotifyUrl}}"\\n`;
                }});
                
                const blob = new Blob([csvContent], {{ type: 'text/csv' }});
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'selected_songs_export.csv';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                window.URL.revokeObjectURL(url);
            }}
            
            // Load saved checkbox states when page loads
            document.addEventListener('DOMContentLoaded', function() {{
                loadCheckboxState();
                
                // Add event listeners to all checkboxes to save state when clicked
                const checkboxes = document.querySelectorAll('input[type="checkbox"]');
                checkboxes.forEach(checkbox => {{
                    checkbox.addEventListener('change', saveCheckboxState);
                }});
            }});
        </script>
    </head>
    <body>
        <div class="container">
            <h1>Multi-Artist Song Grouping Review (First 20 Artists)</h1>
            <div class="summary">
                <h3>Summary: {len(all_artists_data)} Artists Processed</h3>
                <button class="toggle-all" onclick="toggleAll()">Toggle All Artists</button>
            </div>
            
            <div class="export-controls">
                <h4>Export Selected Songs to CSV</h4>
                <button class="select-all-button" onclick="selectAll()">Select All</button>
                <button class="select-all-button" onclick="selectNone()">Select None</button>
                <button class="export-button" onclick="exportToCSV()">Export Selected to CSV</button>
                <p style="margin: 10px 0 0 0; font-size: 12px; color: #666;">
                    Check the songs you want to export, then click "Export Selected to CSV"
                </p>
            </div>
    """
    
    for artist_name, artist_data in all_artists_data.items():
        grouped_songs = artist_data['grouped_songs']
        song_id_to_url = artist_data['song_id_to_url']
        total_groups = len(grouped_songs)
        total_variants = sum(len(song_data['variants']) for song_data in grouped_songs.values())
        
        html += f'''
        <div class="artist-section">
            <div class="artist-header" onclick="toggleArtist('{artist_name}')">
                {artist_name} ({total_groups} groups, {total_variants} total songs)
            </div>
            <div class="artist-content" id="{artist_name}">
        '''
        
        for song_name, song_data in grouped_songs.items():
            html += f'<div class="song-group">'
            html += f'<div class="song-name">{song_name}</div>'
            
            for variant in song_data["variants"]:
                song_id = variant.get("spotify_song_id", variant.get("id", ""))
                spotify_url = song_id_to_url.get(song_id, "#")
                streams = variant.get("total_streams", 0)
                duration = variant.get("duration_ms", 0)
                duration_str = f"{duration//60000}:{(duration%60000)//1000:02d}" if duration else "Unknown"
                
                streams_class = "zero-streams" if streams == 0 else "streams"
                
                html += f'''
                <div class="variant">
                    <input type="checkbox" class="variant-checkbox" data-song-id="{song_id}">
                    <div class="variant-content">
                        <a href="{spotify_url}" target="_blank">{variant["name"]}</a>
                        <div class="album">{variant["album"]}</div>
                        <div class="duration">Duration: {duration_str}</div>
                        <div class="{streams_class}">Streams: {streams:,}</div>
                        <div class="spotify-id">ID: {song_id}</div>
                    </div>
                </div>
                '''
            
            html += '</div>'
        
        html += '</div></div>'
    
    # Add zero streams section
    if zero_streams_songs:
        html += f'''
        <div class="zero-streams-section">
            <div class="zero-streams-header">
                Songs with 0 Streams ({len(zero_streams_songs)} total)
            </div>
        '''
        
        for song in zero_streams_songs:
            spotify_url = song.get("spotify_url", "#")
            spotify_id = song.get("spotify_song_id", "")
            duration = song.get("duration_ms", 0)
            duration_str = f"{duration//60000}:{(duration%60000)//1000:02d}" if duration else "Unknown"
            
            html += f'''
            <div class="zero-streams-song">
                <input type="checkbox" class="variant-checkbox" data-song-id="{spotify_id}">
                <div class="zero-streams-content">
                    <a href="{spotify_url}" target="_blank">{song["name"]}</a>
                    <div class="album">{song["album"]} - {song["primary_artist"]}</div>
                    <div class="duration">Duration: {duration_str}</div>
                    <div class="zero-streams">Streams: 0</div>
                    <div class="spotify-id">ID: {spotify_id}</div>
                </div>
            </div>
            '''
        
        html += '</div>'
    
    html += '</div></body></html>'
    return html

def generate_multi_artist_review(page_number, batch_number, bucket_name):
    """Generate HTML review for the first 20 artists only"""
    try:
        client = storage.Client.from_service_account_json("gcp_creds.json")
        bucket = client.bucket(bucket_name)
        
        # Get artists list
        artists = get_artists_from_gcs(
            bucket_name,
            f"raw-json-data/artists_kworbpage{page_number}/batch{batch_number}/artists.json",
        )
        
        # Only process first 20 artists
        artists_to_process = artists[:20]
        logger.info(f"Processing first {len(artists_to_process)} artists out of {len(artists)} total")
        
        all_artists_data = {}
        zero_streams_songs = []
        
        for artist in tqdm(artists_to_process, desc="Loading artist data"):
            try:
                # Get grouped songs for this artist
                grouped_songs = get_artist_grouped_songs_from_gcs(artist, bucket_name)
                
                # Get individual songs to extract Spotify URLs
                songs = get_artist_songs_from_gcs(artist, bucket_name)
                
                # Create mapping from song ID to Spotify URL
                song_id_to_url = {}
                for song in songs:
                    song_id = song.get("spotify_song_id", "")
                    spotify_url = song.get("spotify_url", "")
                    if song_id and spotify_url:
                        song_id_to_url[song_id] = spotify_url
                    
                    # Collect songs with 0 streams
                    if song.get("total_streams", 0) == 0:
                        zero_streams_songs.append(song)
                
                # Store data for HTML report
                all_artists_data[artist['artist']] = {
                    'grouped_songs': grouped_songs,
                    'song_id_to_url': song_id_to_url,
                    'artist_info': artist
                }
                
            except Exception as e:
                logger.error(f"Error loading data for artist {artist['artist']}: {e}")
                continue
        
        if not all_artists_data:
            logger.error("No artist data found!")
            return
        
        # Create HTML report
        logger.info(f"Creating HTML report for {len(all_artists_data)} artists...")
        logger.info(f"Found {len(zero_streams_songs)} songs with 0 streams")
        review_html = create_multi_artist_review_html(all_artists_data, zero_streams_songs)
        
        # Upload to GCS
        blob_name = f"raw-json-data/artists_kworbpage{page_number}/batch{batch_number}/multi_artist_review_first20.html"
        blob = bucket.blob(blob_name)
        blob.upload_from_string(review_html, content_type="text/html")
        
        logger.info(f"Successfully created multi-artist review HTML at: {blob_name}")
        logger.info(f"Review includes {len(all_artists_data)} artists (first 20)")
        logger.info(f"Zero streams section includes {len(zero_streams_songs)} songs")
        
        # Also save locally for easy access
        local_filename = f"multi_artist_review_first20_page{page_number}_batch{batch_number}.html"
        with open(local_filename, 'w', encoding='utf-8') as f:
            f.write(review_html)
        
        logger.info(f"Also saved locally as: {local_filename}")
        
    except Exception as e:
        logger.error(f"Error generating multi-artist review: {e}")
        raise Exception(f"Error generating multi-artist review: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate multi-artist HTML review for first 20 artists")
    parser.add_argument(
        "--page_number",
        type=int,
        default=1,
        help="The page number of the kworb's page",
    )
    parser.add_argument(
        "--batch_number",
        type=int,
        default=1,
        help="The batch number of the artists",
    )
    parser.add_argument(
        "--bucket_name",
        type=str,
        default="music-ml-data",
        help="GCS bucket name",
    )
    
    args = parser.parse_args()
    
    generate_multi_artist_review(
        args.page_number,
        args.batch_number,
        args.bucket_name
    )