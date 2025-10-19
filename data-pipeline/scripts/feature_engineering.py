import pandas as pd
import numpy as np
import os
import logging

# ------------------------------
# Wilson Lower Bound Calculation
# ------------------------------
def wilson_lower_bound(average_rating, n, confidence=0.95):
    """
    Returns the lower bound of the "true" 5-star rating.
    Penalizes books with fewer ratings due to greater uncertainty.
    """
    if n == 0:
        return 0
    z = 1.96  # for 95% confidence
    x_hat = (average_rating - 1) / 4  # scale to [0, 1]
    wlb = (
        x_hat + z*z/(2*n) - z * np.sqrt((x_hat*(1-x_hat) + z*z/(4*n)) / n)
    ) / (1 + z*z/n)
    return 1 + 4*wlb  # scale back to [1,5]

# ------------------------------
# Feature Engineering Function
# ------------------------------
def engineer_features(df: pd.DataFrame, save=True, out_dir="../data/processed") -> pd.DataFrame:
    """
    Adds engineered features for model training.
    Features:
        - title_length_in_characters
        - title_length_in_words
        - total_ratings
        - adjusted_average_rating (Wilson)
        - great (top 20%)
    """
    logging.info("Starting feature engineering...")
    df = df.copy()

    # --- Title-based features ---
    df["title_length_in_characters"] = df["title"].astype(str).apply(len)
    df["title_length_in_words"] = df["title"].astype(str).apply(lambda x: len(x.split()))

    # --- Ratings ---
    if "ratings_count" in df.columns:
        df["total_ratings"] = df["ratings_count"].astype(int)
    else:
        logging.warning("'ratings_count' not found, defaulting total_ratings = 0")
        df["total_ratings"] = 0

    # --- Wilson-adjusted average rating ---
    df["adjusted_average_rating"] = df.apply(
        lambda x: wilson_lower_bound(x["average_rating"], x["total_ratings"]), axis=1
    )

    # --- Great books flag (top 20%) ---
    threshold = np.percentile(df["adjusted_average_rating"], 80)
    df["great"] = df["adjusted_average_rating"] >= threshold

    # --- Save file ---
    if save:
        os.makedirs(out_dir, exist_ok=True)
        file_path = os.path.join(out_dir, "goodreads_feature_engineered.csv")
        df.to_csv(file_path, index=False)
        logging.info(f"Feature-engineered data saved to {file_path}")

    logging.info("✨ Feature engineering complete.")
    return df

# ------------------------------
# Script Entry Point
# ------------------------------
if __name__ == "__main__":
    input_path = "../data/processed/goodreads_cleaned.csv"
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found at {input_path}")

    raw_df = pd.read_csv(input_path)
    fe_df = engineer_features(raw_df)
    print(fe_df.head())
