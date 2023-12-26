import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_industries(industries: str):
    industry_list = industries.split(', ')

    # Advertising and marketing - Quảng cáo và tiếp thị
    # Agriculture - Nông nghiệp
    # Computer and technology - IT
    # Construction - Xây dựng
    # Education - Giáo dục
    # Energy - Năng lượng
    # Entertainment - Giải trí
    # Fashion - Thời trang
    # Finance and economic - Tài chính và kinh tế
    # Food and beverage - Thực phẩm và đồ uống
    # Health care - Chăm sóc sức khỏe
    # Hospitality - Khách sạn
    # Manufacturing - Sản xuất
    # Media and news - Truyền thông và tin tức
    # Mining - Khai thác mỏ
    # Pharmaceutical - Dược phẩm
    # Telecommunication - Viễn thông
    # Transportation - Vận tải

    # Consulting - Tư vấn
    # Retail and wholesale - bán lẻ/ bán sỉ
    # Customer services - dịch vụ khách hàng
    # Administrative - hành chính
    # Real estate - bất động sản
    # Art - nghệ thuật
    # Foreign trade - ngoại thương
    # Human resources - nhân lực
    # Travel and tourism - du lịch 
    # Insurance - bảo hiểm

    if "Tư vấn" in industry_list[0] :
        return "Consulting"
    if "Dịch vụ khách hàng" in industry_list[0] :
        return "Customer services"
    if "Bán hàng / Kinh doanh" in industry_list[0] :
        return "Retail and wholesale"
    if "Dược / Sinh học" in industry_list[0] :
        return "Pharmaceutical"
    if "Kế toán / Kiểm toán" in industry_list[0] :
        return "Finance and economic"
    if "Quảng cáo / Khuyến mãi / Đối ngoại" in industry_list[0] :
        return "Advertising and marketing"
    if "Ngân hàng / Chứng khoán" in industry_list[0] :
        return "Finance and economic"
    if "Thư ký / Hành chánh" in [0] :
        return "Administrative"
    if "Kỹ thuật ứng dụng / Cơ khí" in industry_list[0] :
        return "Manufacturing"
    if "Chăm sóc sức khỏe / Y tế" in industry_list[0] :
        return "Health care"
    if "Nhà hàng / Dịch vụ ăn uống" in industry_list[0] :
        return "Food and beverage"
    if "Giáo dục / Đào tạo / Thư viện" in industry_list[0] :
        return "Education"
    if "CNTT - Phần mềm" in industry_list[0] :
        return "Computer and technology"
    if "Xây dựng" in industry_list[0] :
        return "Contruction"
    if "Sản xuất / Vận hành sản xuất" in industry_list[0] :
        return "Manufacturing"
    if "Nghệ thuật / Thiết kế / Giải trí" in industry_list[0] :
        return "Art"
    if "Nông nghiệp / Lâm nghiệp" in industry_list[0] :
        return "Agriculture"
    if "Điện / Điện tử" in industry_list[0] :
        return "Manufacturing"
    if "Biên phiên dịch / Thông dịch viên" in industry_list[0] :
        return "Customer services"
    if "Xuất nhập khẩu / Ngoại thương" in industry_list[0] :
        return "Foreign trade"
    if "Quản lý điều hành" in industry_list[0] :
        return "Administrative"
    if "Lao động phổ thông" in industry_list[0] :
        return "Customer services"
    if "Vận chuyển / Giao thông / Kho bãi" in industry_list[0] :
        return "Transportation"
    if "Nhân sự" in industry_list[0] :
        return "Human resources"
    if "Hóa chất / Sinh hóa / Thực phẩm" in industry_list[0] :
        return "Pharmaceutical"
    if "Dệt may / Da giày" in industry_list[0] :
        return "Fashion"
    if "Khách sạn" in industry_list[0] :
        return "Hospitality"
    if "Pháp lý / Luật" in industry_list[0] :
        return "Customer services"
    if "Du lịch" in industry_list[0] :
        return "Travel and tourism"
    if "Bảo trì / Sửa chữa" in industry_list[0] :
        return "Customer services"
    if "Viễn Thông" in industry_list[0] :
        return "Telecommunication"
    if "CNTT - Phần cứng / Mạng" in industry_list[0] :
        return "Computer and technology"
    if "Bất động sản" in industry_list[0] :
        return "Real estate"
    if "An Ninh / Bảo Vệ" in industry_list[0] :
        return "Customer services"
    if "Bán lẻ / Bán sỉ" in industry_list[0] :
        return "Retail and wholesale"
    if "Bảo hiểm" in industry_list[0] :
        return "Insurance"
    if "Tài chính / Đầu tư" in industry_list[0] :
        return "Finance and economic"
    if "Tiếp thị" in industry_list[0] :
        return "Consulting"
    if "Kiến trúc" in industry_list[0] :
        return "Construction"
    if "Nội thất / Ngoại thất" in industry_list[0] :
        return "Manufacturing"
    if "Đồ Gỗ" in industry_list[0] :
        return "Manufacturing"
    if "Thuỷ Hải Sản" in industry_list[0] :
        return "Agriculture"
    if "Ô tô" in industry_list[0] :
        return "Manufacturing"
    if "Dầu khí / Khoáng sản" in industry_list[0] :
        return "Energy"
    if "Thời trang" in industry_list[0] :
        return "Fashion"
    if "Điện lạnh / Nhiệt lạnh" in industry_list[0] :
        return "Manufacturing"
    if "Hàng gia dụng" in industry_list[0] :
        return "Manufacturing"
    return "Other"
