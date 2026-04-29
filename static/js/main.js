// Grocery Analytics Main JS - Production Ready

// Universal AJAX handler
class GroceryAPI {
    static async post(endpoint, data) {
        const response = await fetch(`/api${endpoint}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        });
        return response.json();
    }

    static async get(endpoint) {
        const response = await fetch(`/api${endpoint}`);
        return response.json();
    }
}

// Navbar enhancements
function initNavbar() {
    const currentPath = window.location.pathname.split('/').pop() || 'dashboard';
    
    // Clear existing active
    document.querySelectorAll('.nav-link').forEach(link => link.classList.remove('active'));

    // Find and activate current
    const pageMap = {
        'dashboard': 'Dashboard',
        'checkout': 'Checkout',
        'receipt_center': 'Receipts',
        'market_basket': 'Market Basket',
        'recommendations': 'Recommendations',
        'profit': 'Profit',
        'customers': 'Customers',
        'reports': 'Reports',
        'dataset': 'Dataset',
        'coupons': 'Coupons',
        'admin': 'Admin'
    };

    document.querySelectorAll('.navbar-nav .nav-link').forEach(link => {
        if (link.dataset.route === currentPath || link.textContent.trim() === pageMap[currentPath]) {
            link.classList.add('active');
        }
    });

    // Scroll effect
    let ticking = false;
    function updateNavbar() {
        const navbar = document.querySelector('.navbar');
        if (navbar && window.scrollY > 50) {
            navbar.style.background = 'rgba(17,24,39,0.98)';
            navbar.style.backdropFilter = 'blur(24px) saturate(180%)';
        } else if (navbar) {
            navbar.style.background = '';
            navbar.style.backdropFilter = '';
        }
        ticking = false;
    }
    window.addEventListener('scroll', () => {
        if (!ticking) {
            requestAnimationFrame(updateNavbar);
            ticking = true;
        }
    });
}

// Initialize on DOM ready
document.addEventListener('DOMContentLoaded', initNavbar);

// Checkout page specific
if (document.getElementById('checkout_form')) {
    (function() {
        const currency = '₹';
        const checkboxes = document.querySelectorAll('.product-checkbox');
        const previewContainer = document.getElementById('preview_table');
        const submitBtn = document.getElementById('submit_btn');
        const couponSelect = document.getElementById('coupon_code');
        const couponFeedback = document.getElementById('coupon_feedback');
        const grossTotal = document.getElementById('gross_total');
        const discountAmount = document.getElementById('discount_amount');
        const finalAmount = document.getElementById('final_amount');
        let selectedCount = 0;

        checkboxes.forEach(checkbox => {
            checkbox.addEventListener('change', handleProductChange);
        });

        function handleProductChange() {
            if (this.checked) {
                selectedCount++;
                addToPreview(this);
            } else {
                selectedCount--;
                removeFromPreview(this.value);
            }
            updateSubmitButton();
            updateGrandTotal();
        }

        function addToPreview(checkbox) {
            if (previewContainer.querySelector('.text-muted')) {
                previewContainer.innerHTML = '';
            }
            const row = document.createElement('div');
            row.className = 'product-row row align-items-center mb-2 p-2 border rounded';
            row.dataset.productId = checkbox.value;
            row.dataset.price = checkbox.dataset.price;
            row.innerHTML = `
                <div class="col-6">
                    <strong>${checkbox.dataset.name}</strong>
                </div>
                <div class="col-3">
                    <input type="number" class="form-control qty-input" value="1" min="1" max="10" style="width: 70px;">
                </div>
                <div class="col-3 text-end">
                    <span class="line-total">${currency}${parseFloat(checkbox.dataset.price).toFixed(2)}</span>
                    <button type="button" class="btn btn-sm btn-outline-danger ms-2 remove-item">
                        <i class="fas fa-times"></i>
                    </button>
                </div>
            `;
            previewContainer.appendChild(row);
            row.querySelector('.qty-input').addEventListener('input', () => updatePreviewTotal(row));
            row.querySelector('.remove-item').addEventListener('click', () => removeRow(row));
        }

        function removeRow(row) {
            const checkbox = document.querySelector(`#prod_${row.dataset.productId}`);
            checkbox.checked = false;
            row.remove();
            selectedCount--;
            if (selectedCount === 0) {
                previewContainer.innerHTML = '<p class="text-muted">Select products to preview...</p>';
            }
            updateSubmitButton();
            updateGrandTotal();
        }

        function removeFromPreview(productId) {
            const row = previewContainer.querySelector(`[data-product-id="${productId}"]`);
            if (row) removeRow(row);
        }

        function updatePreviewTotal(row) {
            const qty = Math.max(1, parseInt(row.querySelector('.qty-input').value) || 1);
            row.querySelector('.qty-input').value = qty;
            const total = qty * parseFloat(row.dataset.price);
            row.querySelector('.line-total').textContent = `${currency}${total.toFixed(2)}`;
            updateGrandTotal();
        }

        async function updateGrandTotal() {
            const subtotal = [...previewContainer.querySelectorAll('.product-row')].reduce((sum, row) => {
                return sum + parseFloat(row.querySelector('.line-total').textContent.replace(/[^\d.]/g, ''));
            }, 0);
            
            // Filter coupons by subtotal
            Array.from(couponSelect.options).forEach(option => {
                const minAmount = parseFloat(option.dataset.min || 0);
                option.disabled = minAmount > subtotal && option.value !== '';
                option.style.opacity = minAmount > subtotal ? '0.5' : '1';
            });

            let discount = 0;
            setCouponFeedback('', 'muted');
            
            const selectedCoupon = couponSelect.value;
            if (selectedCoupon && subtotal > 0) {
                const response = await fetch('/api/checkout/validate_coupon', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({total: subtotal, coupon_code: selectedCoupon})
                });
                const result = await response.json();
                discount = result.discount || 0;
                setCouponFeedback(result.message, discount > 0 ? 'success' : 'warning');
            }

            grossTotal.textContent = `${currency}${subtotal.toFixed(2)}`;
            discountAmount.textContent = `${currency}${discount.toFixed(2)}`;
            finalAmount.textContent = `${currency}${Math.max(0, subtotal - discount).toFixed(2)}`;
        }

        function setCouponFeedback(message, type) {
            couponFeedback.textContent = message;
            couponFeedback.className = `coupon-feedback text-${type || 'muted'}`;
        }

        function updateSubmitButton() {
            submitBtn.disabled = selectedCount < 2;
            submitBtn.innerHTML = selectedCount < 2 ?
                '<i class="fas fa-exclamation-triangle me-2"></i>Select 2+ products' :
                '<i class="fas fa-check me-2"></i>Create Transaction';
        }

        couponSelect.addEventListener('change', updateGrandTotal);

        document.getElementById('checkout_form').addEventListener('submit', async function(e) {
            e.preventDefault();
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Creating...';

            const selectedProducts = [...previewContainer.querySelectorAll('.product-row')].map(row => [
                row.dataset.productId,
                parseInt(row.querySelector('.qty-input').value)
            ]).filter(([id, qty]) => qty > 0);

            const formData = new FormData(this);
            const data = {
                customer_id: formData.get('customer_id'),
                basket_date: formData.get('basket_date'),
                payment_method: formData.get('payment_method'),
                selected_products: selectedProducts,
                coupon_code: couponSelect.value
            };

            const response = await GroceryAPI.post('/checkout/create', data);
            if (response.txn_id) {
                showToast(`Transaction ${response.txn_id} created!`, 'success');
                window.location.href = `/receipt/${response.txn_id}`;
            } else {
                alert(response.error || 'Error creating transaction');
                submitBtn.disabled = false;
                updateSubmitButton();
            }
        });
    })();
}

// Toast notifications
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast align-items-center text-white bg-${type === 'success' ? 'success' : type === 'error' ? 'danger' : 'info'} border-0`;
    toast.role = 'alert';
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">${message}</div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    document.body.appendChild(toast);
    new bootstrap.Toast(toast).show();
    toast.addEventListener('hidden.bs.toast', () => toast.remove());
}

// DataTables
function initDataTables() {
    if (typeof $.fn.DataTable !== 'undefined') {
        $('.datatable').DataTable({
            pageLength: 25,
            responsive: true,
            dom: 'Bfrtip',
            buttons: ['copy', 'csv', 'excel', 'pdf']
        });
    }
}

