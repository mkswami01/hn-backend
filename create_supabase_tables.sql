-- HN Newsletter Database Schema for Supabase (MVP)
-- Run this in your Supabase SQL Editor

-- Stories table for HN "Who is hiring" posts
CREATE TABLE IF NOT EXISTS stories (
    id SERIAL PRIMARY KEY,
    hn_id INTEGER UNIQUE NOT NULL,
    title VARCHAR(500),
    month VARCHAR(7) NOT NULL,  -- Format: "2025-01" for efficient filtering
    kids_count INTEGER DEFAULT 0,
    descendants_count INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    created_time TIMESTAMP,
    fetched_time TIMESTAMP DEFAULT NOW()
);

-- Comments table for storing HN job postings
CREATE TABLE IF NOT EXISTS comments (
    id SERIAL PRIMARY KEY,
    hn_id INTEGER UNIQUE NOT NULL,
    story_id INTEGER NOT NULL REFERENCES stories(id),
    story_text TEXT,
    structured_data JSONB,
    processed_status VARCHAR(50) DEFAULT 'pending' CHECK (processed_status IN ('pending', 'processing', 'completed', 'failed')),
    created_time TIMESTAMP,
    fetched_time TIMESTAMP DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_stories_hn_id ON stories(hn_id);
CREATE INDEX IF NOT EXISTS idx_stories_month ON stories(month);  -- Fast month-based queries
CREATE INDEX IF NOT EXISTS idx_comments_hn_id ON comments(hn_id);
CREATE INDEX IF NOT EXISTS idx_comments_story_id ON comments(story_id);
CREATE INDEX IF NOT EXISTS idx_comments_processed_status ON comments(processed_status);

-- Enable Row Level Security (RLS)
ALTER TABLE stories ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;

-- Create policies for public access (adjust as needed)
CREATE POLICY "Allow public read access on stories" ON stories FOR SELECT USING (true);
CREATE POLICY "Allow public insert access on stories" ON stories FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow public read access on comments" ON comments FOR SELECT USING (true);
CREATE POLICY "Allow public insert access on comments" ON comments FOR INSERT WITH CHECK (true);