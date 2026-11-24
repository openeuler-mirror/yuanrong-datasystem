document.addEventListener('DOMContentLoaded', function() {
    console.log('自定义图片灯箱脚本已加载。');

    // --- 创建灯箱所需的DOM元素 ---
    const modalHTML = `
        <div id="custom-image-modal">
            <div id="custom-modal-close">✕</div>
            <img id="custom-modal-image" src="" alt="放大视图">
        </div>
    `;
    document.body.insertAdjacentHTML('beforeend', modalHTML);

    // 获取刚创建的元素
    const modal = document.getElementById('custom-image-modal');
    const closeBtn = document.getElementById('custom-modal-close');
    const modalImg = document.getElementById('custom-modal-image');

    // --- 核心功能函数 ---
    function openModal(imgElement) {
        // 设置放大图片的源
        modalImg.src = imgElement.src;
        modalImg.alt = imgElement.alt || '图片放大视图';
        
        // 显示模态框
        modal.style.display = 'flex';
        // 防止背景滚动
        document.body.style.overflow = 'hidden';
        
        console.log('图片灯箱已打开。');
    }

    function closeModal() {
        modal.style.display = 'none';
        document.body.style.overflow = ''; // 恢复滚动
        console.log('图片灯箱已关闭。');
    }

    // --- 事件绑定 ---
    // 1. 为所有图片绑定点击事件（使用事件委托以提高效率）
    document.addEventListener('click', function(event) {
        // 检查点击的是否为图片，且不在灯箱内
        if (event.target.tagName === 'IMG' && 
            !event.target.closest('#custom-image-modal') &&
            // 可选：排除Logo等不需要放大的图片
            !event.target.closest('header') && 
            !event.target.closest('.logo')) {
            
            event.preventDefault();
            event.stopPropagation();
            openModal(event.target);
        }
    });

    // 2. 点击关闭按钮
    closeBtn.addEventListener('click', function(event) {
        event.stopPropagation(); // 防止事件冒泡到背景
        closeModal();
    });

    // 3. 点击背景（黑色半透明区域）关闭
    modal.addEventListener('click', function(event) {
        // 只有当点击事件发生在背景本身（而不是内部的图片或按钮）上时，才关闭
        if (event.target === modal) {
            closeModal();
        }
    });

    // 4. ESC键关闭
    document.addEventListener('keydown', function(event) {
        if (event.key === 'Escape' && modal.style.display === 'flex') {
            closeModal();
        }
    });

    // 5. 防止模态框内的图片点击触发关闭
    modalImg.addEventListener('click', function(event) {
        event.stopPropagation();
    });

    console.log('图片灯箱初始化完成。');
});