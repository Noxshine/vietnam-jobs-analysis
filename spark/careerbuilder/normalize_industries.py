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
    # Hospitality - Dịch vụ
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

    if "Bán hàng / Kinh doanh" in industry_list[0] :
        return "Retail and wholesale"
    if "Tiếp thị / Marketing" in industry_list[0] :
        return "Advertising and marketing"
    if "Ngân hàng" in industry_list[0] :
        return "Finance and economic"
    if "Kế toán / Kiểm toán" in industry_list[0] :
        return "Finance and economic"
    if "Bán lẻ / Bán sỉ" in industry_list[0] :
        return "Retail and wholesale"
    if "Cơ khí / Ô tô / Tự động hóa" in industry_list[0] :
        return "Manufacturing"
    if "Dịch vụ khách hàng" in industry_list[0] :
        return "Customer services"
    if "Giáo dục / Đào tạo" in industry_list[0] :
        return "Education"
    if "Hành chính / Thư ký" in industry_list[0] :
        return "Administrative"
    if "CNTT - Phần mềm" in industry_list[0] :
        return "Computer and technology"
    if "Điện / Điện tử / Điện lạnh" in industry_list[0] :
        return "Manufacturing"
    if "Luật / Pháp lý" in industry_list[0] :
        return "Customer services"
    if "Sản xuất / Vận hành sản xuất" in industry_list[0] :
        return "Manufacturing"
    if "Tư vấn" in industry_list[0] :
        return "Consulting"
    if "Vận chuyển / Giao nhận /  Kho vận" in industry_list[0] :
        return "Transportation"
    if "Y tế / Chăm sóc sức khỏe" in industry_list[0] :
        return "Health care"
    if "Mỹ thuật / Nghệ thuật / Thiết kế" in industry_list[0] :
        return "Art"
    if "Dược phẩm" in industry_list[0] :
        return "Pharmaceutical"
    if "Quản lý điều hành" in industry_list[0] :
        return "Administrative"
    if "Bảo hiểm" in industry_list[0] :
        return "Customer services"
    if "Thực phẩm & Đồ uống" in industry_list[0] :
        return "Food and beverage"
    if "Kiến trúc" in industry_list[0] :
        return "Construction"
    if "Nhân sự" in industry_list[0] :
        return "Human resources"
    if "Bất động sản" in industry_list[0] :
        return "Real estate"
    if "Biên phiên dịch" in industry_list[0] :
        return "Other"
    if "Dệt may / Da giày / Thời trang" in industry_list[0] :
        return "Fashion"
    if "Xây dựng" in industry_list[0] :
        return "Construction"
    if "Bảo trì / Sửa chữa" in industry_list[0] :
        return "Other"
    if "CNTT - Phần cứng / Mạng" in industry_list[0] :
        return "Computer and technology"
    if "Nhà hàng / Khách sạn" in industry_list[0] :
        return "Hospitality"
    if "Xuất nhập khẩu" in industry_list[0] :
        return "Foreign trade"
    if "Tài chính / Đầu tư" in industry_list[0] :
        return "Finance and economic"
    if "Lao động phổ thông" in industry_list[0] :
        return "Other"
    if "Quảng cáo / Đối ngoại / Truyền Thông" in industry_list[0] :
        return "Advertising and marketing"
    if "Hóa học" in industry_list[0] :
        return "Pharmaceutical"
    if "Nông nghiệp" in industry_list[0] :
        return "Agriculture"
    if "Đồ gỗ" in industry_list[0] :
        return "Manufacturing"
    if "Chứng khoán" in industry_list[0] :
        return "Finance and economic"
    if "Công nghệ thực phẩm / Dinh dưỡng" in industry_list[0] :
        return "Food and beverage"
    if "Hàng gia dụng / Chăm sóc cá nhân" in industry_list[0] :
        return "Customer services"
    if "Tiếp thị trực tuyến" in industry_list[0] :
        return "Consulting"
    if "Công nghệ sinh học" in industry_list[0] :
        return "Pharmaceutical"
    if "Giải trí" in industry_list[0] :
        return  "Media and news"
    if "Du lịch" in industry_list[0] :
        return "Travel and tourism"
    if "Thủy lợi" in industry_list[0] :
        return "Energy"
    if "Hàng hải" in industry_list[0] :
        return "Transportation"
    if "Hàng không" in industry_list[0] :
        return "Transportation"
    if "Lâm Nghiệp" in industry_list[0] :
        return "Agriculture"
    if "Thủy sản / Hải sản" in industry_list[0] :
        return "Agriculture"
    if "Thống kê" in industry_list[0] :
        return "Finance and economic"
    if "Dầu khí" in industry_list[0] :
        return "Energy"
    if "Tổ chức sự kiện" in industry_list[0] :
        return "Customer services"
    
    return "Other"
    