import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

LOAI_BAT_DAU = 0
LOAI_KET_THUC = 1
LOAI_DU_LIEU = 2
LOAI_XAC_NHAN = 3


def dong_goi_ack(so_thu_tu_ack):
    dau_goi = PacketHeader(type=LOAI_XAC_NHAN, seq_num=so_thu_tu_ack, length=0, checksum=0)
    dau_goi.checksum = compute_checksum(dau_goi / b"")
    return bytes(dau_goi / b"")


def tach_va_kiem_tra_goi(goi_tho):
    if len(goi_tho) < 16:
        return None, None
    dau_goi = PacketHeader(goi_tho[:16])
    do_dai_tinh = dau_goi.length
    if do_dai_tinh < 0 or 16 + do_dai_tinh > len(goi_tho):
        return None, None

    noi_dung = goi_tho[16 : 16 + do_dai_tinh]
    crc_nhan_duoc = dau_goi.checksum
    dau_goi.checksum = 0
    if compute_checksum(dau_goi / noi_dung) != crc_nhan_duoc:
        return None, None
    return dau_goi, noi_dung


def receiver(receiver_ip, receiver_port, window_size):
    """Lang nghe tren cong UDP va phan hoi ACK rieng le cho tung goi DATA."""
    cong_nhan = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    cong_nhan.bind((receiver_ip, receiver_port))

    trang_thai_ket_noi = False
    stt_mong_doi = 1
    bo_dem_nhan = {}

    while True:
        du_lieu_den, dia_chi_gui = cong_nhan.recvfrom(2048)
        dau_goi, noi_dung = tach_va_kiem_tra_goi(du_lieu_den)
        if dau_goi is None:
            continue

        loai_goi = dau_goi.type
        stt_goi = dau_goi.seq_num

        if loai_goi == LOAI_BAT_DAU:
            if not trang_thai_ket_noi and stt_goi == 0:
                trang_thai_ket_noi = True
                stt_mong_doi = 1
                bo_dem_nhan.clear()
                cong_nhan.sendto(dong_goi_ack(1), dia_chi_gui)
            elif trang_thai_ket_noi and stt_mong_doi == 1:
                cong_nhan.sendto(dong_goi_ack(1), dia_chi_gui)
            continue

        if not trang_thai_ket_noi:
            continue

        if loai_goi == LOAI_DU_LIEU:
            nguong_cua_so = stt_mong_doi + window_size
            if stt_goi >= nguong_cua_so:
                continue

            if stt_goi >= stt_mong_doi and stt_goi not in bo_dem_nhan:
                bo_dem_nhan[stt_goi] = noi_dung

            while stt_mong_doi in bo_dem_nhan:
                manh_hop_le = bo_dem_nhan.pop(stt_mong_doi)
                sys.stdout.buffer.write(manh_hop_le)
                sys.stdout.buffer.flush()
                stt_mong_doi += 1

            cong_nhan.sendto(dong_goi_ack(stt_goi), dia_chi_gui)

        elif loai_goi == LOAI_KET_THUC:
            if stt_goi == stt_mong_doi:
                cong_nhan.sendto(dong_goi_ack(stt_mong_doi + 1), dia_chi_gui)
                break

    cong_nhan.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
