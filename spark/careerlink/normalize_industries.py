import pyspark.sql.functions as F
import pyspark.sql.types as T


@F.udf(returnType=T.StringType())
def normalize_industries(industries: str):
    industry_list = industries.split(', ')

    # Advertising and marketing - Quảng cáo và tiếp thị
    # Aerospace - Hàng không
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
    # Hospitality - Dịch vụ
    # Manufacturing - Sản xuất
    # Media and news - Truyền thông và tin tức
    # Mining - Khai thác mỏ
    # Pharmaceutical - Dược phẩm
    # Telecommunication - Viễn thông
    # Transportation - Vận tải

    if "Bán hàng / Kinh doanh" in industry_list[0] :
        return "Finance and economic"
    if "Kiến trúc" in industry_list[0] :
        return "Construction "
    if "Tư vấn" in industry_list[0] :
        return "Hospitality"
    if "Dịch vụ khách hàng" in industry_list[0] :
        return "Hospitality"
    if "Tài chính / Đầu tư" in industry_list[0] :
        return "Finance and economic"
    if "Bảo hiểm" in industry_list[0] :
        return "Hospitality"
    if "Ngân hàng / Chứng khoán" in industry_list[0] :
        return "Finance and economic"
    if "Mới tốt nghiệp / Thực tập" in industry_list[0] :
        return "Other"
    if "Bán lẻ / Bán sỉ" in industry_list[0] :
        return "Finance and economic"
    if "Giáo dục / Đào tạo / Thư viện" in industry_list[0] :
        return "Education"
    if "Kế toán / Kiểm toán" in industry_list[0] :
        return "Finance and economic"
    if "Điện / Điện tử" in industry_list[0] :
        return "Computer and technology"
    if "Kỹ thuật ứng dụng / Cơ khí" in industry_list[0] :
        return "Manufacturing"
    if "Sản xuất / Vận hành sản xuất" in industry_list[0] :
        return "Manufacturing"
    if "Thư ký / Hành chánh" in industry_list[0] :
        return "Other"
    if "CNTT - Phần mềm" in industry_list[0] :
        return "Computer and technology"
    if "Biên phiên dịch / Thông dịch viên" in industry_list[0] :
        return "Other"
    if "Tiếp thị" in industry_list[0] :
        return "Advertising and marketing"
    if "Quảng cáo / Khuyến mãi / Đối ngoại" in industry_list[0] :
        return "Advertising and marketing"
    if "Dược / Sinh học" in industry_list[0] :
        return "Pharmaceutical"
    if "Biên phiên dịch (tiếng Nhật)" in industry_list[0] :
        return "Other"
    if "Lao động phổ thông" in industry_list[0] :
        return "Other"
    if "Hóa chất / Sinh hóa / Thực phẩm" in industry_list[0] :
        return "Pharmaceutical"
    if "Quản lý chất lượng (QA / QC)" in industry_list[0] :
        return "Manufacturing"
    if "Vận chuyển / Giao thông / Kho bãi" in industry_list[0] :
        return "Transportation "
    if "Thời trang" in industry_list[0] :
        return "Fashion "
    if "Xây dựng" in industry_list[0] :
        return "Construction"
    if "Bảo trì / Sửa chữa" in industry_list[0] :
        return "Manufacturing"
    if "CNTT - Phần cứng / Mạng" in industry_list[0] :
        return "Computer and technology"
    if "Quản lý điều hành" in industry_list[0] :
        return "Other"
    if "Nhân sự" in industry_list[0] :
        return "Other"
    if "Nhà hàng / Dịch vụ ăn uống" in industry_list[0] :
        return "Food and beverage"
    if "Chăm sóc sức khỏe / Y tế" in industry_list[0] :
        return "Health care"
    if "Dệt may / Da giày" in industry_list[0] :
        return "Manufacturing"
    if "Báo chí / Biên tập viên / Xuất bản" in industry_list[0] :
        return "Media and news"
    if "Xuất nhập khẩu / Ngoại thương" in industry_list[0] :
        return "Transportation"
    if "Nghệ thuật / Thiết kế / Giải trí" in industry_list[0] :
        return "Entertainment"
    if "Điện lạnh / Nhiệt lạnh" in industry_list[0] :
        return "Energy"
    if "Vật tư / Thu mua" in industry_list[0] :
        return "Manufacturing"
    if "Bất động sản" in industry_list[0] :
        return "Construction"
    if "Nội thất / Ngoại thất" in industry_list[0] :
        return "Manufacturing"
    if "Pháp lý / Luật" in industry_list[0] :
        return "Other"
    if "Viễn Thông" in industry_list[0] :
        return "Telecommunication"
    if "Khác" in industry_list[0] :
        return "Other"
    if "Môi trường / Xử lý chất thải" in industry_list[0] :
        return "Other"
    if "An Toàn Lao Động" in industry_list[0] :
        return "Other"
    if "Nông nghiệp / Lâm nghiệp" in industry_list[0] :
        return "Agriculture"
    if "Du lịch" in industry_list[0] :
        return "Hospitality"
    if "Hàng gia dụng" in industry_list[0] :
        return "Manufacturing"
    if "Đồ Gỗ" in industry_list[0] :
        return "Manufacturing"
    if "Khách sạn" in industry_list[0] :
        return "Hospitality"
    if "An Ninh / Bảo Vệ" in industry_list[0] :
        return "Other"
    if "Ô tô" in industry_list[0] :
        return "Manufacturing"
    if "Dầu khí / Khoáng sản" in industry_list[0] :
        return "Mining"
    if "Thuỷ Hải Sản" in industry_list[0] :
        return "Agriculture"
    if "Người nước ngoài" in industry_list[0] :
        return "Other"
  
    return "Other"