import os
import re
import pandas as pd
import numpy as np

from src.config import *


class DataCleaner:
    """
    Cleans and transforms raw scraped property data into a structured format.
    It loads the raw CSV and image map (optionally), processes the data using a series
    of extraction functions, and saves the final result to an Excel file.
    """

    def __init__(self):
        """Initializes the DataCleaner with paths from the config file."""
        self.raw_details_path = DETAILS_OUTPUT_PATH
        self.image_map_path = IMAGE_MAP_CSV_PATH
        self.output_path = CLEANED_DETAILS_OUTPUT_PATH
        self.df = None
        self.cleaned_df = None

    def load_data(self):
        """
        Loads the raw data and image map (if available), then merges them.
        Includes deduplication by 'property_id'.
        """
        if not os.path.exists(self.raw_details_path):
            raise FileNotFoundError(f"Raw details file not found: {self.raw_details_path}")

        df = pd.read_csv(self.raw_details_path)
        df.columns = [
            "listing_title", "property_id", "total_price", "unit_price",
            "property_url", "image_url", "city", "district", "alley_width",
            "features", "property_description"
        ]
        
        # Load image map if it exists, else create empty 'screenshot_url' column
        if os.path.exists(self.image_map_path):
            image_map_df = pd.read_csv(self.image_map_path)
            merged_df = pd.merge(df, image_map_df, how="left", on="property_id")
            print(f"[INFO] Loaded and merged image map with {len(image_map_df)} records.")
        else:
            print(f"[WARN] Image map file not found: {self.image_map_path}. Proceeding without screenshot URLs.")
            merged_df = df.copy() 
            merged_df['screenshot_url'] = np.nan 

        if not merged_df.empty:
            merged_df = merged_df.iloc[:-1]  # intentional removal of last row 

        # Deduplicate by 'property_id', keeping the first occurrence
        initial_rows = len(merged_df)
        self.df = merged_df.drop_duplicates(subset=['property_id'], keep='first')
        
        if len(self.df) < initial_rows:
            print(f"[INFO] Deduplicated {initial_rows - len(self.df)} records by 'property_id'.")

        print(f"[INFO] Loaded and prepared {len(self.df)} records for cleaning.")

    # -- Helper Methods (no changes needed here as they operate on self.df columns) --
    @staticmethod
    def _extract_city(row):
        if pd.notna(row.get("city")):
            return str(row["city"]).replace("TP.", "Thành phố").strip()

        title = str(row.get("listing_title", ""))
        match = re.search(r"(TP\.|Thành phố)\s*([^.,\n]+)", title, re.IGNORECASE)

        return f"Thành phố {match.group(2).strip()}" if match else np.nan

    @staticmethod
    def _extract_district(row):
        district = row.get("district")

        if pd.notna(district):
            return str(district).replace("Q.", "Quận").replace("H.", "Huyện").replace("TX.", "Thị xã").strip()

        title = str(row.get("listing_title", ""))
        match = re.search(r"\b(Q\.|H\.|TX\.)\s*([^.,\n]+)", title, re.IGNORECASE)

        if match:
            prefix, name = match.group(1).upper(), match.group(2).strip()
            if prefix == "Q.":
                return f"Quận {name}"
            if prefix == "H.":
                return f"Huyện {name}"
            if prefix == "TX.":
                return f"Thị xã {name}"

        return np.nan

    @staticmethod
    def _extract_location(df):
        def extract_row(row):
            full_address = row['listing_title']
            district = row['district']

            if pd.isna(full_address) or not isinstance(full_address, str) or \
                    pd.isna(district) or not isinstance(district, str):
                return np.nan

            pattern_str = r",\s*((?:P|X|TT)\.\s*[^,]+?)\s*,\s*" + re.escape(district)
            match = re.search(pattern_str, full_address)

            def standardize_prefix(location_str):
                location_str = location_str.strip()
                if location_str.startswith("P."):
                    return location_str.replace("P.", "Phường", 1).strip()
                elif location_str.startswith("X."):
                    return location_str.replace("X.", "Xã", 1).strip()
                elif location_str.startswith("TT."):
                    return location_str.replace("TT.", "Thị trấn", 1).strip()
                return location_str

            if match:
                return standardize_prefix(match.group(1))

            parts = [part.strip() for part in full_address.split(',')]
            try:
                district_index = -1
                for i, part_val in enumerate(parts):
                    if part_val == district:
                        district_index = i
                        break

                if district_index > 0:
                    location = parts[district_index - 1]
                    if location.upper().startswith(("P.", "X.", "TT.")):
                        return standardize_prefix(location)

            except (IndexError, ValueError):
                pass

            return np.nan

        return df.apply(extract_row, axis=1)

    @staticmethod
    def _extract_street_name(series: pd.Series) -> pd.Series:
        def extract(text: str):
            if pd.isna(text):
                return np.nan

            patterns = [
                r"(?:Nhà mặt ngõ|Đất nền|Nhà trong ngõ).*?cách\s+(.*?)\s*\d+(?:\.\d+)?m",
                r"(?:Nhà mặt phố|Mặt đường)\s+([^,]+?)\s*,",
                r"Đất nền\s+((?!.*cách)[^,]+?)\s*,"
            ]

            for p in patterns:
                if match := re.search(p, str(text), re.IGNORECASE):
                    return re.sub(r'\s*\(.*\)\s*$', '', match.group(1).strip()).strip()
            return np.nan

        return series.apply(extract)

    @staticmethod
    def _classify_property_type(title: str) -> str:
        if pd.isna(title):
            return ""

        if "cách" in title:
            return "Mặt ngõ"

        return "Mặt phố"

    @staticmethod
    def _convert_price_to_numeric(price_str: str) -> float:
        if pd.isna(price_str):
            return np.nan

        price = str(price_str).lower()

        try:
            val_str = price.replace(',', '.').strip()
            if 'tỷ' in val_str:
                return float(val_str.replace('tỷ', '').strip()) * 1e9
            if 'triệu' in val_str:
                return float(val_str.replace('triệu', '').strip()) * 1e6
            return float(val_str)
        except (ValueError, AttributeError):
            return np.nan

    @staticmethod
    def _estimate_price(price: float) -> float:
        return round(price * 0.98, 2) if pd.notna(price) else np.nan

    @staticmethod
    def _extract_alley_width(row):
        for text in [row.get("alley_width"), row.get("property_description")]:
            if pd.notna(text):
                if nums := re.findall(r"(\d+(?:\.\d+)?)", str(text)):
                    try:
                        return min(float(n) for n in nums)
                    except ValueError:
                        continue
        return np.nan

    @staticmethod
    def _extract_front_width(row):
        sources = [row.get('features'), row.get('property_description')]
        patterns = [
            r"Hướng mặt tiền\s*:[^;-]+?-\s*(\d+(?:\.\d+)?)\s*m",
            r"Nhà mặt tiền\s+(\d+(?:\.\d+)?)\s*m"
        ]
        for text in sources:
            if pd.notna(text):
                for p in patterns:
                    if match := re.search(p, str(text), re.IGNORECASE):
                        try:
                            return float(match.group(1))
                        except ValueError:
                            pass
        return np.nan

    @staticmethod
    def _extract_number_of_floors (row):
        title = str(row.get("listing_title", "")).lower()
        if "đất nền" in title:
            return 0.0

        text = str(row.get("features", "")) + " " + str(row.get("property_description", ""))
        floor = re.search(r"Số tầng:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        basement = re.search(r"Số tầng hầm:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        total_floors = 0.0
        found = False

        if floor:
            total_floors += float(floor.group(1))
            found = True
        if basement:
            total_floors += float(basement.group(1))
            found = True

        return total_floors if found else np.nan

    @staticmethod
    def _extract_land_area(row):
        text = str(row.get("features", "")) + " " + str(row.get("property_description", ""))
        patterns = [r"Diện tích:\s*(\d+(?:\.\d+)?)", r"diện tích đất thực tế là\s*([\d.]+)m²"]

        for p in patterns:
            if match := re.search(p, text, re.IGNORECASE):
                try:
                    return float(match.group(1))
                except (ValueError, IndexError):
                    pass

        return np.nan

    @staticmethod
    def _extract_distance_to_main_road(row):
        desc, title = str(row.get('property_description', '')), str(row.get('listing_title', ''))

        if 'mặt phố' in title.lower():
            return 0

        patterns = [
            r"khoảng cách ra trục đường chính\s*(\d+(?:\.\d+)?)\s*m",
            r"cách\s+.*?\s+(\d+(?:\.\d+)?)\s*m"
        ]
        text_sources = [desc, title]

        for text, p in zip(text_sources, patterns):
            if match := re.search(p, text, re.IGNORECASE):
                return float(match.group(1))

        return 0

    @staticmethod
    def _extract_number_of_frontages(row):
        """
        Parses the property description to find the number of frontages.
        Defaults to 1 if not explicitly mentioned.
        """
        text = row.get("property_description")
        if pd.isna(text):
            return 1

        # Search for patterns like "2 mặt tiền", "3 mặt tiền", etc.
        match = re.search(r"(\d+)\s*mặt tiền", str(text), re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                pass

        # If no explicit number is found (e.g., "Nhà mặt tiền"), default to 1.
        return 1

    @staticmethod
    def _estimate_remaining_quality(row):
        title = str(row.get("listing_title", "")).lower()

        if "đất nền" in title:
            return ""
        return 0.85

    @staticmethod
    def _estimate_construction_price(row):
        text = str(row.get("features", "")) + " " + str(row.get("property_description", ""))
        floor = re.search(r"Số tầng:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        basement = re.search(r"Số tầng hầm:\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
        total_floors = 0.0

        if "đất nền" in text:
            return ""

        # Safely extract and convert to float, default to 0 if not found
        total_floors += float(floor.group(1)) if floor else 0.0
        total_floors += float(basement.group(1)) if basement else 0.0

        if total_floors == 1:
            return 6275876

        # Check if basement existed to apply basement price
        if basement and float(basement.group(1)) > 0: # Ensure basement was actually found and has floors
            return 9504604

        # Default for multi-floor without specific basement criteria met
        if total_floors > 1: # Added condition to prevent '8221171' for 'đất nền'
            return 8221171
        
        return np.nan # For 'đất nền' or cases where no construction price can be estimated

    def clean_data(self):
        """Applies all cleaning and transformation steps to the loaded data."""
        if self.df is None:
            self.load_data()

        city = self.df.apply(self._extract_city, axis=1)
        district = self.df.apply(self._extract_district, axis=1)
        location = self._extract_location(self.df)
        street = self._extract_street_name(self.df["listing_title"])
        prop_type = self.df["listing_title"].apply(self._classify_property_type)
        price = self.df["total_price"].apply(self._convert_price_to_numeric)
        est_price = price.apply(self._estimate_price)
        floors = self.df.apply(self._extract_number_of_floors, axis=1)
        num_frontages = self.df.apply(self._extract_number_of_frontages, axis=1)
        area = self.df.apply(self._extract_land_area, axis=1)
        front_width = self.df.apply(self._extract_front_width, axis=1)
        remaining_quality = self.df.apply(self._estimate_remaining_quality, axis=1)
        construction_price = self.df.apply(self._estimate_construction_price, axis=1)

        with np.errstate(divide='ignore', invalid='ignore'):
            # If `floors` is NaN, fill with 1.0, so `total_area` becomes equal to `area`.
            # If `floors` is 0 (for "Đất nền"), `total_area` correctly becomes 0.
            floors_for_calc = floors.fillna(1.0)
            total_area = round((floors_for_calc * area).replace([np.inf, -np.inf], np.nan), 2)
            length = round((area / front_width).replace([np.inf, -np.inf], np.nan), 2)

        self.cleaned_df = pd.DataFrame({
            "Tỉnh/Thành phố": city,
            "Quận/Huyện/Thị xã": district,
            "Xã/Phường/Thị trấn": location,
            "Đường phố": street,
            "Địa chỉ chi tiết": prop_type,
            "Nguồn thông tin": self.df["property_url"],
            "Tình trạng giao dịch": "Chưa giao dịch",
            "Thời điểm giao dịch/rao bán": pd.NaT,
            "Thông tin liên hệ": "",
            "Giá rao bán/giao dịch": price,
            "Giá ước tính": est_price,
            "Loại đơn giá (đ/m2 hoặc đ/m ngang)": "đ/m2",
            "Đơn giá đất": "",
            "Số tầng công trình": floors,
            "Chất lượng còn lại": remaining_quality,
            "Giá trị công trình xây dựng": "",
            "Đơn giá xây dựng": construction_price,
            "Diện tích đất (m2)": area,
            "Tổng diện tích sàn": total_area,
            "Kích thước mặt tiền (m)": front_width,
            "Kích thước chiều dài": length,
            "Số mặt tiền tiếp giáp": num_frontages,
            "Hình dạng": "Chữ nhật",
            "Độ rộng ngõ/ngách nhỏ nhất (m)": self.df.apply(self._extract_alley_width, axis=1),
            "Khoảng cách tới trục đường chính (m)": self.df.apply(self._extract_distance_to_main_road, axis=1),
            "Mục đích sử dụng đất": "Đất ở",
            "Hình ảnh của bài đăng": self.df["image_url"],
            "Ảnh chụp màn hình thông tin thu thập": self.df["screenshot_url"], # This column will now contain NaNs if file doesn't exist
            "Yếu tố khác": ""
        })
        print("[INFO] Data cleaning process completed.")

    def save_cleaned_data(self):
        """Saves the cleaned DataFrame to an Excel file."""
        if self.cleaned_df is None:
            print("[ERROR] No cleaned data to save. Run clean_data() first.")
            return

        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        self.cleaned_df.to_excel(self.output_path, index=False, engine='openpyxl')
        print(f"[INFO] Cleaned data saved to {self.output_path}")