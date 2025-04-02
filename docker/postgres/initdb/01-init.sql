-- Create a dummy table for demonstration
CREATE TABLE IF NOT EXISTS dummy_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Insert some sample data
INSERT INTO dummy_table (name) VALUES
    ('First Entry'),
    ('Second Entry'),
    ('Third Entry'); 